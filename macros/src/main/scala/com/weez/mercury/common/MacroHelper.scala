package com.weez.mercury.common

import scala.language.experimental.macros
import scala.reflect.macros.whitebox.Context

trait MacroHelper {
  val c: Context

  import c.universe._

  def withFirst(annottees: Seq[c.Expr[Any]])(f: => Tree): c.Expr[Any] = {
    if (annottees.length != 1) {
      c.error(c.enclosingPosition, "expect case class")
      c.Expr[Any](EmptyTree)
    } else {
      c.Expr[Any](f)
    }
  }

  def ensureInherit(parents: List[Tree], check: Tree) = {
    val baseType = c.typecheck(check, c.TYPEmode).tpe
    val inherit = parents exists { p =>
      c.typecheck(p, c.TYPEmode).tpe <:< baseType
    }
    if (inherit) parents else parents :+ check
  }

  def ensureParam(paramss: List[List[Tree]], p: Tree) = {
    val q"$_ val $name: $tpe = $_" = p
    paramss.flatten collectFirst {
      case q"$_ val $xname: $xtpe = $_" if xname.toString == name.toString =>
        val pt = c.typecheck(tpe, c.TYPEmode).tpe
        if (!(c.typecheck(xtpe, c.TYPEmode).tpe <:< pt))
          c.error(xtpe.pos, "expect type " + pt.typeSymbol.fullName)
    } match {
      case Some(x) =>
        paramss
      case None =>
        (p :: paramss.head) :: paramss.tail
    }
  }

  def withClass(tree: Tree, flags: Option[FlagSet] = None)(f: (TypeName, List[List[Tree]], List[Tree], List[Tree], Modifiers, List[Tree], Modifiers) => Tree): Tree = {
    try {
      tree match {
        case q"$mods class $tpname[..$tparams] $ctorMods(...$paramss) extends ..$parents { ..$body }" =>
          if (flags.isEmpty || mods.hasFlag(flags.get)) {
            f(tpname, paramss, parents, body, mods, tparams, ctorMods)
          } else {
            c.error(tree.pos, "expect class with " + flags)
            EmptyTree
          }
        case _ =>
          c.error(tree.pos, "expect class")
          EmptyTree
      }
    } catch {
      case ex: PositionedException =>
        c.error(ex.pos, ex.getMessage)
        EmptyTree
    }
  }

  def makeFunctionTypeWithParamList(paramss: List[List[Tree]], returnType: Tree): Tree = {
    makeFunctionType(paramss map { params =>
      params map {
        case q"$mods val $name: $tpe = $_" => tpe
      }
    }, returnType)
  }

  def makeFunctionType(paramss: List[List[Tree]], returnType: Tree): Tree = {
    paramss match {
      case Nil => tq"() => $returnType"
      case params :: Nil =>
        params.length match {
          case 0 => tq"() => $returnType"
          case 1 => tq"Function1[${params(0)}, $returnType]"
          case 2 => tq"Function2[${params(0)}, ${params(1)}, $returnType]"
          case 3 => tq"Function3[${params(0)}, ${params(1)}, ${params(2)}, $returnType]"
          case 4 => tq"Function4[${params(0)}, ${params(1)}, ${params(2)}, ${params(3)}, $returnType]"
          case 5 => tq"Function5[${params(0)}, ${params(1)}, ${params(2)}, ${params(3)}, ${params(4)}, $returnType]"
          case 6 => tq"Function6[${params(0)}, ${params(1)}, ${params(2)}, ${params(3)}, ${params(4)}, ${params(5)}, $returnType]"
          case 7 => tq"Function7[${params(0)}, ${params(1)}, ${params(2)}, ${params(3)}, ${params(4)}, ${params(5)}, ${params(6)}, $returnType]"
          case 8 => tq"Function8[${params(0)}, ${params(1)}, ${params(2)}, ${params(3)}, ${params(4)}, ${params(5)}, ${params(6)}, ${params(7)}, $returnType]"
          case 9 => tq"Function9[${params(0)}, ${params(1)}, ${params(2)}, ${params(3)}, ${params(4)}, ${params(5)}, ${params(6)}, ${params(7)}, ${params(8)}, $returnType]"
          case 10 => tq"Function10[${params(0)}, ${params(1)}, ${params(2)}, ${params(3)}, ${params(4)}, ${params(5)}, ${params(6)}, ${params(7)}, ${params(8)}, ${params(9)}, $returnType]"
          case 11 => tq"Function11[${params(0)}, ${params(1)}, ${params(2)}, ${params(3)}, ${params(4)}, ${params(5)}, ${params(6)}, ${params(7)}, ${params(8)}, ${params(9)}, ${params(10)}, $returnType]"
          case 12 => tq"Function12[${params(0)}, ${params(1)}, ${params(2)}, ${params(3)}, ${params(4)}, ${params(5)}, ${params(6)}, ${params(7)}, ${params(8)}, ${params(9)}, ${params(10)}, ${params(11)}, $returnType]"
          case 13 => tq"Function13[${params(0)}, ${params(1)}, ${params(2)}, ${params(3)}, ${params(4)}, ${params(5)}, ${params(6)}, ${params(7)}, ${params(8)}, ${params(9)}, ${params(10)}, ${params(11)}, ${params(12)}, $returnType]"
          case 14 => tq"Function14[${params(0)}, ${params(1)}, ${params(2)}, ${params(3)}, ${params(4)}, ${params(5)}, ${params(6)}, ${params(7)}, ${params(8)}, ${params(9)}, ${params(10)}, ${params(11)}, ${params(12)}, ${params(13)}, $returnType]"
          case 15 => tq"Function15[${params(0)}, ${params(1)}, ${params(2)}, ${params(3)}, ${params(4)}, ${params(5)}, ${params(6)}, ${params(7)}, ${params(8)}, ${params(9)}, ${params(10)}, ${params(11)}, ${params(12)}, ${params(13)}, ${params(14)}, $returnType]"
          case 16 => tq"Function16[${params(0)}, ${params(1)}, ${params(2)}, ${params(3)}, ${params(4)}, ${params(5)}, ${params(6)}, ${params(7)}, ${params(8)}, ${params(9)}, ${params(10)}, ${params(11)}, ${params(12)}, ${params(13)}, ${params(14)}, ${params(15)}, $returnType]"
          case 17 => tq"Function17[${params(0)}, ${params(1)}, ${params(2)}, ${params(3)}, ${params(4)}, ${params(5)}, ${params(6)}, ${params(7)}, ${params(8)}, ${params(9)}, ${params(10)}, ${params(11)}, ${params(12)}, ${params(13)}, ${params(14)}, ${params(15)}, ${params(16)}, $returnType]"
          case 18 => tq"Function18[${params(0)}, ${params(1)}, ${params(2)}, ${params(3)}, ${params(4)}, ${params(5)}, ${params(6)}, ${params(7)}, ${params(8)}, ${params(9)}, ${params(10)}, ${params(11)}, ${params(12)}, ${params(13)}, ${params(14)}, ${params(15)}, ${params(16)}, ${params(17)}, $returnType]"
          case 19 => tq"Function19[${params(0)}, ${params(1)}, ${params(2)}, ${params(3)}, ${params(4)}, ${params(5)}, ${params(6)}, ${params(7)}, ${params(8)}, ${params(9)}, ${params(10)}, ${params(11)}, ${params(12)}, ${params(13)}, ${params(14)}, ${params(15)}, ${params(16)}, ${params(17)}, ${params(18)}, $returnType]"
          case 20 => tq"Function20[${params(0)}, ${params(1)}, ${params(2)}, ${params(3)}, ${params(4)}, ${params(5)}, ${params(6)}, ${params(7)}, ${params(8)}, ${params(9)}, ${params(10)}, ${params(11)}, ${params(12)}, ${params(13)}, ${params(14)}, ${params(15)}, ${params(16)}, ${params(17)}, ${params(18)}, ${params(19)}, $returnType]"
          case 21 => tq"Function21[${params(0)}, ${params(1)}, ${params(2)}, ${params(3)}, ${params(4)}, ${params(5)}, ${params(6)}, ${params(7)}, ${params(8)}, ${params(9)}, ${params(10)}, ${params(11)}, ${params(12)}, ${params(13)}, ${params(14)}, ${params(15)}, ${params(16)}, ${params(17)}, ${params(18)}, ${params(19)}, ${params(20)}, $returnType]"
          case 22 => tq"Function22[${params(0)}, ${params(1)}, ${params(2)}, ${params(3)}, ${params(4)}, ${params(5)}, ${params(6)}, ${params(7)}, ${params(8)}, ${params(9)}, ${params(10)}, ${params(11)}, ${params(12)}, ${params(13)}, ${params(14)}, ${params(15)}, ${params(16)}, ${params(17)}, ${params(18)}, ${params(19)}, ${params(20)}, ${params(21)}, $returnType]"
        }
      case head :: tail =>
        makeFunctionType(head :: Nil, makeFunctionType(tail, returnType))
    }
  }

  def flattenFunction(name: Tree, paramss: List[List[Tree]]): Tree = {
    val params = paramss.flatten map {
      case q"$mod val $name: $tpe = $_" =>
        ValDef(Modifiers(Flag.PARAM), name, tpe, EmptyTree)
    }
    Function(params, q"$name(...$paramss)")
  }

  def getAnnotation(expectName: String, defaults: Seq[(String, Any)]): Map[String, Any] = {
    c.prefix.tree match {
      case q"new $tpname(...$paramss)" =>
        // http://stackoverflow.com/questions/24602433/macro-annotations-and-type-parameter
        // https://github.com/scalamacros/paradise/issues/14
        val actualType = c.typecheck(tpname, c.TYPEmode).tpe
        val actualName = actualType.typeSymbol.fullName
        if (expectName != actualName)
          throw new PositionedException(tpname.pos, s"expect $expectName\nfound $actualName")
        var i = 0
        val builder = Map.newBuilder[String, Any]
        builder ++= defaults
        paramss foreach { params =>
          params foreach { p =>
            p match {
              case Literal(Constant(value)) =>
                val (prop, _) = defaults(i)
                builder += prop -> value
              case AssignOrNamedArg(Ident(TermName(prop)), Literal(Constant(value))) =>
                builder += prop -> value
              case _ =>
                throw new PositionedException(p.pos, "expect literal or named-argument with literal")
            }
            i += 1
          }
        }
        builder.result()
    }
  }

  def importName(name: String): c.Tree = {
    val arr = name.split("\\.")
    if (arr.length < 2)
      EmptyTree
    else {
      var expr: Tree = Ident(TermName(arr(0)))
      for (i <- 1 until arr.length - 1)
        expr = Select(expr, TermName(arr(i)))
      val name = TermName(arr(arr.length - 1))
      Import(expr, List(ImportSelector(name, -1, name, -1)))
    }
  }

  def println(a: Any): Unit = {
    c.warning(c.enclosingPosition, if (a == null) "<null>" else a.toString)
  }

  class PositionedException(val pos: Position, message: String) extends Exception(message)

}
