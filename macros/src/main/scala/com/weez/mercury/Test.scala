package com.weez.mercury


import spray.json._

import scala.reflect.runtime.{universe => ru}

object Test {
  def main(args: Array[String]) = {
    f(Test)
  }

  import scala.reflect.runtime.universe._

  private def f[T: TypeTag](rc: T): Unit = {
    val tag = implicitly[TypeTag[T]]
    println(tag.tpe.typeSymbol.fullName)
  }


}

