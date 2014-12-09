package com.weez.mercury


import spray.json._

import scala.reflect.runtime.{universe => ru}

object Test {
  def main(args: Array[String]) = {
    import PersonJsonSupport._

    val p = Person(Name("Jim", "Green"))
    println(p.toJson)
  }
}

