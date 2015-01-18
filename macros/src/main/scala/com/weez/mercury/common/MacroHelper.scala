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

  def typecheckWorkaround0(tree: Tree, mode: c.TypecheckMode): Option[Type] = {
    // http://stackoverflow.com/questions/24602433/macro-annotations-and-type-parameter
    // https://github.com/scalamacros/paradise/issues/14
    try {
      Some(c.typecheck(tree, c.TYPEmode).tpe)
    } catch {
      case ex: c.TypecheckException =>
        tree match {
          case Ident(TypeName(_)) => // maybe local name
          case _ => c.warning(tree.pos, ex.getMessage)
        }
        None
    }
  }

  def typecheckWorkaround1(tree: Tree, mode: c.TypecheckMode): Type = {
    typecheckWorkaround0(tree, c.TYPEmode) match {
      case Some(x) => x
      case None => c.abort(tree.pos, "type not found")
    }
  }

  def findInherit(parents: List[Tree], expect: Tree): Option[Tree] = {
    val baseType = typecheckWorkaround1(expect, c.TYPEmode)
    parents find { p =>
      typecheckWorkaround0(p, c.TYPEmode) match {
        case Some(x) => x <:< baseType
        case None => false
      }
    }
  }

  def findParam(paramss: List[List[Tree]], name: String, expectType: Option[Tree] = None): Option[Tree] = {
    paramss.flatten collectFirst {
      case p@q"$_ val $name0: $tpe = $_" if name0.toString == name =>
        expectType match {
          case Some(x) =>
            val expect = typecheckWorkaround1(x, c.TYPEmode)
            val typeEqual =
              typecheckWorkaround0(tpe, c.TYPEmode) match {
                case Some(x) => x <:< expect
                case None => false
              }
            if (!typeEqual)
              c.error(tpe.pos, "expect type " + expect.typeSymbol.fullName)
          case None =>
        }
        p
    }
  }

  def camelCase2sepStyle(name: String): String = {
    import scala.util.matching.Regex
    new Regex("[A-Z]+").replaceAllIn(name, { m =>
      if (m.start == 0)
        m.matched.toLowerCase
      else
        "-" + m.matched.toLowerCase
    })
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

  def evalAnnotation[T: TypeTag](): T = {
    c.prefix.tree match {
      case q"new $_(...$paramss)" =>
        val name = typeNameOf(typeOf[T].typeSymbol.fullName)
        c.eval(c.Expr[T](q"new $name(...$paramss)"))
    }
  }

  def typeNameOf(name: String): Tree = {
    import scala.annotation.tailrec
    @tailrec
    def makeName(parts: List[String], tree: Tree): Tree = {
      parts match {
        case x :: Nil => Select(tree, TypeName(x))
        case x :: tail => makeName(tail, Select(tree, TermName(x)))
        case _ => throw new IllegalArgumentException
      }
    }
    val parts = name.split("\\.").toList
    makeName(if (parts.head == "_root_") parts.tail else parts, Ident(termNames.ROOTPKG))
  }

  def importName(name: String): Tree = {
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
