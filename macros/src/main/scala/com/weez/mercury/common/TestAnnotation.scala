package com.weez.mercury.common

import scala.language.experimental.macros
import scala.annotation.StaticAnnotation
import scala.reflect.macros.whitebox.Context

case class TestAnnotation(rootCollection: Boolean)

class test extends StaticAnnotation {
  def macroTransform(annottees: Any*): Any = macro TestMacro.impl
}

class TestMacro(val c: Context) extends MacroHelper {

  import c.universe._

  def impl(annottees: c.Expr[Any]*): c.Expr[Any] = {

    c.Expr[Any](annottees.head.tree)
  }
}