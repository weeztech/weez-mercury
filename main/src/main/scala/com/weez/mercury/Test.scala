package com.weez.mercury

object Test {

  @common.dbtype
  case class A(abc: String) extends AutoCloseable {
    def close(): Unit = ???
  }


  class B(id: Int)

}
