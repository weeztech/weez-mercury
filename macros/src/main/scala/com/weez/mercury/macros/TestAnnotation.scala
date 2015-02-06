package com.weez.mercury.macros

import scala.language.experimental.macros
import scala.annotation.StaticAnnotation
import scala.reflect.macros.blackbox

case class TestAnnotation(rootCollection: Boolean)

class test extends StaticAnnotation {
  def macroTransform(annottees: Any*): Any = macro TestMacro.impl
}

object TestMacros {
  def test[A]: Unit = macro TestMacro.testImpl[A]
}

class TestMacro(val c: blackbox.Context) extends MacroHelper {

  import c.universe._

  def impl(annottees: c.Tree*): c.Tree = {
    annottees.head
  }

  def testImpl[T: c.WeakTypeTag]: c.Tree = {
    withException {
      val tpe = weakTypeOf[T]
      val symbol = tpe.typeSymbol.asClass
      val subs = scala.collection.mutable.ListBuffer[Symbol]()
      def findAllSubClasses(x: ClassSymbol): Unit = {
        val direct = x.knownDirectSubclasses
        subs.appendAll(direct)
        direct foreach {
          case x: ClassSymbol =>
            if (x.isSealed || x.isFinal)
              findAllSubClasses(x.asClass)
            else
              throw new PositionedException(x.pos, "expect sealed or final")
          case x: ModuleSymbol =>
            subs.append(x)
        }
      }
      findAllSubClasses(symbol)
      warn(subs)
      q"com.weez.mercury.common.TestMacros.hello"
    }
  }
}