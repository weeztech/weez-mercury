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
        AppliedTypeTree(Ident(TypeName("Function" + params.length)), params :+ returnType)
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
