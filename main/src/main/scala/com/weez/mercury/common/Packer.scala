package com.weez.mercury.common

import scala.annotation.tailrec

trait Packer[T] {
  def apply(value: T): Array[Byte] = {
    val buf = new Array[Byte](packLength(value))
    pack(value, buf, 0)
    buf
  }

  def unapply(buf: Array[Byte]): T = unpack(buf, 0, buf.length)

  def pack(value: T, buf: Array[Byte], offset: Int)

  def packLength(value: T): Int

  def unpack(buf: Array[Byte], offset: Int, length: Int): T

  def unpackLength(buf: Array[Byte], offset: Int): Int
}

object Packer {
  val TYPE_STRING: Byte = 1
  val TYPE_INT: Byte = 2
  val TYPE_LONG: Byte = 3
  val TYPE_TUPLE: Byte = 10
  val TYPE_END: Byte = 0

  def pack[T](value: T)(implicit packer: Packer[T]) = packer(value)

  def unpack[T](buf: Array[Byte])(implicit packer: Packer[T]) = packer.unapply(buf)

  abstract class FixedLengthPacker[T](length: Int) extends Packer[T] {
    def packLength(value: T) = length

    def unpackLength(buf: Array[Byte], offset: Int) = length
  }

  val charset = java.nio.charset.Charset.forName("UTF-8")

  implicit object StringPacker extends Packer[String] {
    def pack(value: String, buf: Array[Byte], offset: Int) = {
      val arr = value.getBytes(charset)
      require(buf.length >= offset + arr.length, "buffer too small")
      buf(offset) = TYPE_STRING
      buf(buf.length - 1) = TYPE_END
      System.arraycopy(arr, 0, buf, offset + 1, arr.length)
    }

    def packLength(value: String): Int = {
      // http://stackoverflow.com/questions/8511490/calculating-length-in-utf-8-of-java-string-without-actually-encoding-it
      var count = 0
      var i = 0
      while (i < value.length) {
        val ch = value.charAt(i)
        if (ch <= 0x7F) {
          count += 1
        } else if (ch <= 0x7FF) {
          count += 2
        } else if (Character.isHighSurrogate(ch)) {
          count += 4
          i += 1
        } else {
          count += 3
        }
        i += 1
      }
      count + 2
    }

    def unpack(buf: Array[Byte], offset: Int, length: Int) = {
      require(buf(offset) == TYPE_STRING, "not a string")
      require(buf(offset + length - 1) == 0, "invalid length")
      new String(buf, offset + 1, length - 2, charset).intern()
    }

    def unpackLength(buf: Array[Byte], offset: Int): Int = {
      var end = offset
      while (end < buf.length && buf(end) != TYPE_END)
        end += 1
      require(end < buf.length, "partial string")
      end + 1 - offset
    }
  }

  implicit object IntPacker extends FixedLengthPacker[Int](5) {
    def pack(value: Int, buf: Array[Byte], offset: Int) = {
      require(buf.length >= offset + 5, "buffer too small")
      buf(offset) = TYPE_INT
      buf(offset + 1) = (value >> 24).asInstanceOf[Byte]
      buf(offset + 2) = (value >> 16).asInstanceOf[Byte]
      buf(offset + 3) = (value >> 8).asInstanceOf[Byte]
      buf(offset + 4) = value.asInstanceOf[Byte]
    }

    def unpack(buf: Array[Byte], offset: Int, length: Int) = {
      require(buf(offset) == TYPE_INT, "not a Int")
      require(buf.length >= offset + 5 && length == 5, "invalid length")
      ((buf(offset + 1) & 0xff) << 24) |
        ((buf(offset + 2) & 0xff) << 16) |
        ((buf(offset + 3) & 0xff) << 8) |
        (buf(offset + 4) & 0xff)
    }
  }

  implicit object LongPacker extends FixedLengthPacker[Long](9) {
    def pack(value: Long, buf: Array[Byte], offset: Int) = {
      require(buf.length >= offset + 9, "buffer too small")
      buf(offset) = TYPE_LONG
      buf(offset + 1) = (value >> 56).asInstanceOf[Byte]
      buf(offset + 2) = (value >> 48).asInstanceOf[Byte]
      buf(offset + 3) = (value >> 40).asInstanceOf[Byte]
      buf(offset + 4) = (value >> 32).asInstanceOf[Byte]
      buf(offset + 5) = (value >> 24).asInstanceOf[Byte]
      buf(offset + 6) = (value >> 16).asInstanceOf[Byte]
      buf(offset + 7) = (value >> 8).asInstanceOf[Byte]
      buf(offset + 8) = value.asInstanceOf[Byte]
    }

    def unpack(buf: Array[Byte], offset: Int, length: Int) = {
      require(buf(offset) == TYPE_LONG, "not a Long")
      require(buf.length >= offset + 9 && length == 9, "invalid length")
      ((buf(offset + 1) & 0xffL) << 56) |
        ((buf(offset + 2) & 0xffL) << 48) |
        ((buf(offset + 3) & 0xffL) << 40) |
        ((buf(offset + 4) & 0xffL) << 32) |
        ((buf(offset + 5) & 0xffL) << 24) |
        ((buf(offset + 6) & 0xffL) << 16) |
        ((buf(offset + 7) & 0xffL) << 8) |
        (buf(offset + 8) & 0xffL)
    }
  }

  class ProductPacker[T <: Product](packers: Packer[_]*) extends Packer[T] {
    def pack(value: T, buf: Array[Byte], offset: Int) = {
      buf(offset) = TYPE_TUPLE
      var itor = value.productIterator
      var end = offset + 1
      packers foreach { p =>
        val v = itor.next().asInstanceOf[_]
        p.pack(v, buf, end)
        end += p.packLength(v)
      }
      buf(end) = 0
    }

    def packLength(value: T): Int = {
      var itor = value.productIterator
      packers.foldLeft(2) { (c, p) =>
        c + p.packLength(itor.next().asInstanceOf[_])
      }
    }

    def unpack(buf: Array[Byte], offset: Int, length: Int) = {
      require(buf(offset) == TYPE_TUPLE, "not a Tuple")
      var end = offset + 1
      packers map { p =>
        val len = p.unpackLength(buf, end)
        val v = p.unpack(buf, end, len)
        end += len
        v
      }
    }

    def unpackLength(buf: Array[Byte], offset: Int) = {
      var i = offset + 1
      packers foreach { p =>
        i = p.findEnd(buf, i) + 1
      }
      i
    }
  }

}