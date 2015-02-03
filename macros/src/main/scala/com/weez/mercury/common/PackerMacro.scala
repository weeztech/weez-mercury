package com.weez.mercury.common

import scala.annotation.StaticAnnotation
import scala.language.experimental.macros
import scala.reflect.macros.whitebox

class packable extends StaticAnnotation {
  def macroTransform(annottees: Any*): Any = macro PackerMacro.packableImpl
}

class tuplePackers extends StaticAnnotation {
  def macroTransform(annottees: Any*): Any = macro PackerMacro.tupleImpl
}

class caseClassPackers extends StaticAnnotation {
  def macroTransform(annottees: Any*): Any = macro PackerMacro.caseClassImpl
}

trait PackerMacros {
  def poly[T, A]: Any = macro PackerMacro.polyImpl[T, A]
}

class PackerMacro(val c: whitebox.Context) extends MacroHelper {

  import c.universe._

  def packableImpl(annottees: c.Tree*): c.Tree = {
    withException {
      annottees.head match {
        case q"${mods: Modifiers} class ${name: TypeName}[..${tparams: List[Tree]}](...${paramss: List[List[Tree]]}) extends ..$_ { ..$_ }" if mods.hasFlag(Flag.CASE) =>
          val packer = flattenFunction(q"apply", paramss)
          var companionBody =
            if (tparams.isEmpty) {
              q"implicit val packer = _root_.com.weez.mercury.common.Packer.caseClass($packer)" :: Nil
            } else {
              val names = (tparams map {
                case TypeDef(_, name, _, _) => name
              }).toSet
              val builder = List.newBuilder[Tree]
              var i = 0
              paramss foreach {
                _ foreach {
                  case ValDef(_, _, tpt, _) =>
                    val isGenericType =
                      tpt match {
                        case AppliedTypeTree(Ident(x: TypeName), _) => names.contains(x)
                        case Ident(x: TypeName) => names.contains(x)
                      }
                    if (isGenericType) {
                      builder += ValDef(Modifiers(Flag.PARAM | Flag.IMPLICIT),
                        TermName("$implicit$p" + i), tq"_root_.com.weez.mercury.common.Packer[$tpt]", EmptyTree)
                      i += 1
                    }
                }
              }
              val params = builder.result()
              q"implicit def packer[..$tparams](..$params) = _root_.com.weez.mercury.common.Packer.caseClass($packer)" :: Nil
            }
          val all =
            annottees.head ::
              q"object ${name.toTermName} { ..$companionBody }" ::
              Nil
          //println(show(q"..$all"))
          q"..$all"
        case _ => throw new PositionedException(annottees.head.pos, "expect case class or sealed trait")
      }
    }
  }

  val packerType = tq"Packer"

  def tupleImpl(annottees: c.Expr[Any]*): c.Expr[Any] = {
    c.Expr[Any](refactBody(annottees.head.tree) {
      _ ++ makeTupleFunctions()
    })
  }

  def makeTupleFunctions() = {
    def makeTupleApply(n: Int, prefix: String) = {
      Apply(Select(Ident(TermName("Tuple" + n)), TermName("apply")), (0 until n).toList map { i => Ident(TermName(prefix + i))})
    }

    def makeTypeArgs(n: Int) = {
      var typeArgs: List[Tree] = Nil
      for (i <- 0 until n) {
        val name = ('A' + (n - i - 1)).asInstanceOf[Char]
        typeArgs = Ident(TypeName(name.toString)) :: typeArgs
      }
      typeArgs
    }

    def makeTypeArgsDef(typeArgs: List[Tree]) = {
      typeArgs map {
        case Ident(x: TypeName) =>
          TypeDef(Modifiers(Flag.PARAM), x, List(), TypeBoundsTree(EmptyTree, EmptyTree))
      }
    }

    def makeParams(typeArgs: List[Tree]) = {
      var params: List[ValDef] = Nil
      val n = typeArgs.length
      for (i <- 0 until n) {
        val j = n - i - 1
        val p = ValDef(Modifiers(Flag.PARAM | Flag.IMPLICIT), TermName("p" + j), tq"$packerType[${typeArgs(j)}]", EmptyTree)
        params = p :: params
      }
      params
    }

    for (i <- 1 to 22) yield {
      val name = TermName(s"imp_tuple$i")
      val typeArgs = makeTypeArgs(i)
      val typeArgsDef = makeTypeArgsDef(typeArgs)
      val params = makeParams(typeArgs)
      val tupleType = AppliedTypeTree(Ident(TypeName("Tuple" + i)), typeArgs)
      val paramsWithIndex = params.zipWithIndex
      val f_pack =
        q"buf(offset) = TYPE_TUPLE" ::
          q"var end = offset + 1" ::
          (paramsWithIndex map { case (p, pi) =>
            q"end = ${p.name}.pack(value.${TermName("_" + (pi + 1))}, buf, end)"
          }) :::
          q"buf(end) = TYPE_END" ::
          q"end + 1" :: Nil
      val f_packLength =
        q"var len = 2" ::
          (paramsWithIndex map { case (p, pi) =>
            q"len += ${p.name}.packLength(value.${TermName("_" + (pi + 1))})"
          }) :::
          q"len" :: Nil
      val f_unpack =
        q"""require(buf(offset) == TYPE_TUPLE, "not a tuple")""" ::
          q"var end = offset + 1" ::
          q"var len = 0" ::
          (paramsWithIndex map { case (p, pi) =>
            q"len = ${p.name}.unpackLength(buf, end)" ::
              q"val ${TermName("t" + pi)} = ${p.name}.unpack(buf, end, len)" ::
              q"end += len" :: Nil
          }).flatten :::
          makeTupleApply(paramsWithIndex.length, "t") :: Nil
      val f_unpackLength =
        q"var end = offset + 1" ::
          (params map { p => q"end += ${p.name}.unpackLength(buf, end)"}) :::
          q"end + 1 - offset" :: Nil
      val tree =
        q"""
        implicit def $name[..$typeArgsDef](..$params): $packerType[$tupleType] = {
          new $packerType[$tupleType] {
            def pack(value: $tupleType, buf: Array[Byte], offset: Int) = { ..$f_pack }
            def packLength(value: $tupleType) = { ..$f_packLength }
            def unpack(buf: Array[Byte], offset: Int, length: Int) = { ..$f_unpack }
            def unpackLength(buf: Array[Byte], offset: Int) = { ..$f_unpackLength }
          }
        }
       """
      tree
    }
  }

  def caseClassImpl(annottees: c.Expr[Any]*): c.Expr[Any] = {
    c.Expr[Any](refactBody(annottees.head.tree) {
      _ ++ makeCaseClassFunctions()
    })
  }

  def makeCaseClassFunctions() = {
    def makeTypeArgs(n: Int) = {
      var typeArgs: List[Tree] = Nil
      for (i <- 0 until n) {
        val name = ('A' + (n - i - 1)).asInstanceOf[Char]
        typeArgs = Ident(TypeName(name.toString)) :: typeArgs
      }
      typeArgs
    }

    def makeTypeArgsDef(typeArgs: List[Tree]) = {
      TypeDef(Modifiers(Flag.PARAM), TypeName("Z"), List(), TypeBoundsTree(EmptyTree, Ident(TypeName("Product")))) ::
        (typeArgs map {
          case Ident(x: TypeName) =>
            TypeDef(Modifiers(Flag.PARAM), x, List(), TypeBoundsTree(EmptyTree, EmptyTree))
        })
    }

    def makeParams(typeArgs: List[Tree]) = {
      var params: List[ValDef] = Nil
      val n = typeArgs.length
      for (i <- 0 until n) {
        val j = n - i - 1
        val p = ValDef(Modifiers(Flag.PARAM | Flag.IMPLICIT), TermName("p" + j), tq"$packerType[${typeArgs(j)}]", EmptyTree)
        params = p :: params
      }
      params
    }

    for (i <- 1 to 22) yield {
      val typeArgs = makeTypeArgs(i)
      val typeArgsDef = makeTypeArgsDef(typeArgs)
      val params = makeParams(typeArgs)
      val paramsWithIndex = params.zipWithIndex
      val f_pack =
        q"buf(offset) = TYPE_TUPLE" ::
          q"var end = offset + 1" ::
          q"val itor = value.productIterator" ::
          (paramsWithIndex map { case (p, pi) =>
            q"end = ${p.name}.pack(itor.next().asInstanceOf[${typeArgs(pi)}], buf, end)"
          }) :::
          q"buf(end) = TYPE_END" ::
          q"end + 1" :: Nil
      val f_packLength =
        q"var len = 2" ::
          q"val itor = value.productIterator" ::
          (paramsWithIndex map { case (p, pi) =>
            q"len += ${p.name}.packLength(itor.next().asInstanceOf[${typeArgs(pi)}])"
          }) :::
          q"len" :: Nil
      val f_unpack =
        q"""require(buf(offset) == TYPE_TUPLE, "not a tuple")""" ::
          q"var end = offset + 1" ::
          q"var len = 0" ::
          (paramsWithIndex map { case (p, pi) =>
            q"len = ${p.name}.unpackLength(buf, end)" ::
              q"val ${TermName("a" + pi)} = ${p.name}.unpack(buf, end, len)" ::
              q"end += len" :: Nil
          }).flatten :::
          q"f(..${paramsWithIndex map { case (_, pi) => Ident(TermName("a" + pi))}})" :: Nil
      val f_unpackLength =
        q"var end = offset + 1" ::
          (params map { p => q"end += ${p.name}.unpackLength(buf, end)"}) :::
          q"end + 1 - offset" :: Nil
      val tree =
        q"""
        def caseClass[..$typeArgsDef](f: (..$typeArgs) => Z)(..$params): $packerType[Z] = {
          new $packerType[Z] {
            def pack(value: Z, buf: Array[Byte], offset: Int) = { ..$f_pack }
            def packLength(value: Z) = { ..$f_packLength }
            def unpack(buf: Array[Byte], offset: Int, length: Int) = { ..$f_unpack }
            def unpackLength(buf: Array[Byte], offset: Int) = { ..$f_unpackLength }
          }
        }
       """
      //println(show(tree))
      tree
    }
  }

  def polyImpl[T: c.WeakTypeTag, A: c.WeakTypeTag]: c.Tree = {
    val packerType = tq"_root_.com.weez.mercury.common.Packer"
    withException {
      val baseType = weakTypeOf[T]
      expect(baseType.typeArgs.isEmpty, c.enclosingPosition, s"expect no type arguments: $baseType")
      val types = typesInHList(weakTypeOf[A])
      val subs: List[(String, Tree, Tree)] = types map { tpe =>
        expect(tpe <:< baseType, c.enclosingPosition, s"expect subtype of $baseType: $tpe")
        val symbol = tpe.typeSymbol.asClass
        if (symbol.isModuleClass) {
          (camelCase2sepStyle(symbol.name.toString), symbolTree(symbol.selfType.termSymbol.asModule, false), null)
        } else {
          expect(tpe.typeArgs.isEmpty, c.enclosingPosition, s"expect no type arguments: ${symbol.fullName}")
          expect(symbol.isCaseClass, c.enclosingPosition, s"expect case class: ${symbol.fullName}")
          (camelCase2sepStyle(symbol.name.toString), symbolTree(symbol, true), Select(symbolTree(symbol, false), TermName("packer")))
        }
      }
      val f_pack =
        Match(Ident(TermName("value")), subs map { tp =>
          if (tp._3 == null) {
            CaseDef(tp._2, EmptyTree,
              Block(q"buf(offset) = TYPE_TUPLE" ::
                q"val end = of[String].pack(${Literal(Constant(tp._1))}, buf, offset)" ::
                q"buf(end) = TYPE_END" :: Nil,
                q"end + 1"))
          } else {
            CaseDef(Bind(TermName("x"), Typed(Ident(termNames.WILDCARD), tp._2)), EmptyTree,
              Block(q"buf(offset) = TYPE_TUPLE" ::
                q"var end = of[String].pack(${Literal(Constant(tp._1))}, buf, offset)" ::
                q"end = ${tp._3}.pack(x, buf, end)" ::
                q"buf(end) = TYPE_END" :: Nil,
                q"end + 1"))
          }
        }) :: Nil
      val f_packLength =
        Match(Ident(TermName("value")), subs map { tp =>
          if (tp._3 == null) {
            CaseDef(tp._2, EmptyTree,
              q"2 + of[String].packLength(${Literal(Constant(tp._1))})")
          } else {
            CaseDef(Bind(TermName("x"), Typed(Ident(termNames.WILDCARD), tp._2)), EmptyTree,
              q"2 + of[String].packLength(${Literal(Constant(tp._1))}) + ${tp._3}.packLength(x)")
          }
        }) :: Nil
      val f_unpack =
        q"""require(buf(offset) == TYPE_TUPLE, "not a tuple")""" ::
          q"var end = offset + 1" ::
          q"var len = of[String].unpackLength(buf, end)" ::
          q"val tpe = of[String].unpack(buf, end, len)" ::
          q"end += len" ::
          Match(Ident(TermName("tpe")), subs map { tp =>
            CaseDef(Literal(Constant(tp._1)), EmptyTree,
              if (tp._3 == null) {
                Block( q"""require(buf(end) == TYPE_END, "invalid length")""" :: Nil,
                  tp._2)
              } else {
                Block(q"len = ${tp._3}.unpackLength(buf, end)" ::
                  q"val v = ${tp._3}.unpack(buf, end, len)" ::
                  q"end += len" ::
                  q"""require(buf(end) == TYPE_END, "invalid length")""" :: Nil,
                  q"v")
              })
          }) :: Nil
      val baseTypeTree = symbolTree(baseType.typeSymbol, true)
      val tree =
        q"""new $packerType[$baseTypeTree] {
                  import _root_.com.weez.mercury.common.Packer._
                  def pack(value: $baseTypeTree, buf: Array[Byte], offset: Int) = { ..$f_pack }
                  def packLength(value: $baseTypeTree) = { ..$f_packLength }
                  def unpack(buf: Array[Byte], offset: Int, length: Int): $baseTypeTree = { ..$f_unpack }
                  def unpackLength(buf: Array[Byte], offset: Int) = { getLengthByType(buf, offset) }
                }
             """
      tree
    }
  }
}
