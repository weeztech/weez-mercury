package com.weez.mercury


import spray.json._

import scala.reflect.runtime.{universe => ru, ReflectionUtils}

object Test {
  def main(args: Array[String]): Unit = {
    val mirror = ru.runtimeMirror(this.getClass.getClassLoader)
    val pkg = mirror.staticPackage("com.weez")

    import scala.language.dynamics


    def f(a: Double, b: String, c: Int) = ???


    //    def get: Query

    val tp = ("", 1)
    tp match {
      case (name, id) => name + (id + 1)
    }
    //    select { r =>
    //      (r.name, r.code, r.price, r.items.select(i => (i.name, i.code)))
    //    }.map {
    //      case (name, code, price, items: List[(String, String)]) =>
    //        (name + 1, code, items)
    //    }.range()
  }

}

