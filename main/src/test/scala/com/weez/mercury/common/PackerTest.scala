package com.weez.mercury.common

import org.scalatest._

class PackerTest extends FlatSpec with Matchers {

  import Packer._
  import Util._

  def buf(i: Byte) = {
    val arr = new Array[Byte](1)
    arr(0) = i
    arr
  }

  "A tuple Packer" should "pack TupleX to Array[Byte]" in {
    val tp = (10, "abc")
    val arr = pack(tp)
    var arr2 = concatUInt8s(buf(TYPE_TUPLE), pack(tp._1), pack(tp._2), buf(TYPE_END))
    assert(arr.isInstanceOf[Array[Byte]] && Util.compareUInt8s(arr, arr2) == 0)
  }

  it should "unpack an Array[Byte] to TupleX" in {
    val tp = (10, "abc")
    var arr = concatUInt8s(buf(TYPE_TUPLE), pack(tp._1), pack(tp._2), buf(TYPE_END))
    assert(unpack[(Int, String)](arr) == tp)
  }

}
