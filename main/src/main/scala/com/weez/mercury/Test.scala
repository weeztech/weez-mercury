package com.weez.mercury

object Test {

  @common.test(false)
  case class A(abc: String) extends AutoCloseable {
    def close(): Unit = ???
  }


  class B(id: Int)

}
