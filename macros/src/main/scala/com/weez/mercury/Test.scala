package com.weez.mercury


import spray.json._

import scala.reflect.runtime.{universe => ru, ReflectionUtils}

object Test {
  def main(args: Array[String]) = {
    val mirror = ru.runtimeMirror(this.getClass.getClassLoader)
    val pkg = mirror.staticPackage("com.weez")

    import scala.language.dynamics

    val a = new A
    val name = a.name[String]

  }

  import scala.language.dynamics

  class A extends Dynamic {
    def selectDynamic[T](name: String): T = {
      ???
    }
  }

}

