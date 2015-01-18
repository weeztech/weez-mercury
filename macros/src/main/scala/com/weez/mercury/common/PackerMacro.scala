package com.weez.mercury.common

import scala.annotation.StaticAnnotation
import scala.language.experimental.macros
import scala.reflect.macros.whitebox.Context

class tuplePackers extends StaticAnnotation {
  def macroTransform(annottees: Any*): Any = macro PackerMacro.tupleImpl
}

class caseClassPackers extends StaticAnnotation {
  def macroTransform(annottees: Any*): Any = macro PackerMacro.caseClassImpl
}

class PackerMacro(val c: Context) extends MacroHelper {

  import c.universe._

  def tupleImpl(annottees: c.Expr[Any]*): c.Expr[Any] = {
    val tree =
      annottees.head.tree match {
        case x: ClassDef =>
          ClassDef(x.mods, x.name, x.tparams, Template(x.impl.parents, x.impl.self, x.impl.body ++ makeTupleFunctions()))
        case x: ModuleDef =>
          ModuleDef(x.mods, x.name, Template(x.impl.parents, x.impl.self, x.impl.body ++ makeTupleFunctions()))
        case x =>
          c.error(x.pos, "expect class/trait/object")
          annottees.head.tree
      }
    c.Expr[Any](tree)
  }

  val packerType = tq"_root_.com.weez.mercury.common.Packer"

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
      val name = TermName(s"tuple$i")
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
          import com.weez.mercury.common.Packer.{TYPE_TUPLE, TYPE_END}
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
    val tree =
      annottees.head.tree match {
        case x: ClassDef =>
          ClassDef(x.mods, x.name, x.tparams, Template(x.impl.parents, x.impl.self, x.impl.body ++ makeCaseClassFunctions()))
        case x: ModuleDef =>
          ModuleDef(x.mods, x.name, Template(x.impl.parents, x.impl.self, x.impl.body ++ makeCaseClassFunctions()))
        case x =>
          c.error(x.pos, "expect class/trait/object")
          annottees.head.tree
      }
    c.Expr[Any](tree)
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
        def apply[..$typeArgsDef](f: (..$typeArgs) => Z)(..$params): $packerType[Z] = {
          import com.weez.mercury.common.Packer.{TYPE_TUPLE, TYPE_END}
          new $packerType[Z] {
            def pack(value: Z, buf: Array[Byte], offset: Int) = { ..$f_pack }
            def packLength(value: Z) = { ..$f_packLength }
            def unpack(buf: Array[Byte], offset: Int, length: Int) = { ..$f_unpack }
            def unpackLength(buf: Array[Byte], offset: Int) = { ..$f_unpackLength }
          }
        }
       """
      tree
    }
  }
}
