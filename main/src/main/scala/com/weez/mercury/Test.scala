package com.weez.mercury

object Test {

  @common.test(false)
  case class A(abc: String) extends AutoCloseable {
    def close(): Unit = ???

    trait C

  }

  object A {

    trait B

  }

  trait B {

    trait C

    object C

  }

  object B {


  }

  def main(args: Array[String]): Unit = {
    import scala.reflect.runtime.{universe => ru}
    val start = System.nanoTime()

    val classpath = ClassFinder.classpath.filter { f =>
      f.isDirectory() || f.getName.contains("weez")
    }

    val mirror = ru.runtimeMirror(this.getClass.getClassLoader)
    val classes = ClassFinder.scalaNamesIn(classpath)
    import ClassFinder._
    classes foreach {
      case ScalaName(name, _, true, false) =>
        println(name)
        mirror.staticClass(name)
      case _ =>
    }

    println("done: " + (System.nanoTime() - start) / 1000000)
  }
}
