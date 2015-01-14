package com.weez.mercury.common

import scala.language.experimental.macros
import scala.annotation.StaticAnnotation
import scala.reflect.macros.whitebox.Context

class test(rootCollection: Boolean) extends StaticAnnotation {
  def macroTransform(annottees: Any*): Any = macro TestMacro.impl
}

class TestMacro(val c: Context) extends MacroHelper {
  def impl(annottees: c.Expr[Any]*): c.Expr[Any] = {
    import c._
    val tree = annottees.head.tree

    c.Expr[Any](tree)
  }
}