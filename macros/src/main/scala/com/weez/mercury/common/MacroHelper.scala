package com.weez.mercury.common

import scala.language.experimental.macros
import scala.reflect.macros.whitebox.Context

trait MacroHelper {
  val c: Context

  import c.universe._

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

  def refactBody(tree: Tree)(f: List[Tree] => List[Tree]): Tree = {
    tree match {
      case x: ClassDef =>
        ClassDef(x.mods, x.name, x.tparams, Template(x.impl.parents, x.impl.self, f(x.impl.body)))
      case x: ModuleDef =>
        ModuleDef(x.mods, x.name, Template(x.impl.parents, x.impl.self, f(x.impl.body)))
      case x =>
        c.error(x.pos, "expect class/trait/object")
        tree
    }
  }

  def withException(f: => Tree): c.Expr[Any] = {
    try {
      c.Expr(f)
    } catch {
      case ex: PositionedException =>
        c.error(ex.pos, ex.getMessage)
        c.Expr(EmptyTree)
    }
  }

  def expect(condition: Boolean, pos: c.Position, message: String) = {
    if (!condition) throw new PositionedException(pos, message)
  }

  def expect[T](tree: Tree, message: String): T = {
    try {
      tree.asInstanceOf[T]
    } catch {
      case ex: ClassCastException =>
        throw new PositionedException(tree.pos, message)
    }
  }

  def expectClass(tree: Tree): ClassDef = {
    expect[ClassDef](tree, "expect class/trait")
  }

  def expectCaseClass(tree: Tree): ClassDef = {
    val c = expect[ClassDef](tree, "expect case class")
    expect(c.mods.hasFlag(Flag.CASE), c.pos, "expect case class")
    c
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
    val argss = paramss map {
      _ map {
        case q"$_ val $name: $_ = $_" => q"$name"
      }
    }
    Function(params, q"$name(...$argss)")
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
