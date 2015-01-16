package com.weez.mercury.common

import scala.language.implicitConversions

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

object Packer extends ProductPackers with CollectionPackers {
  val TYPE_STRING: Byte = 1
  val TYPE_INT: Byte = 2
  val TYPE_LONG: Byte = 3
  val TYPE_FALSE: Byte = 4
  val TYPE_TRUE: Byte = 5
  val TYPE_RAW: Byte = 6
  val TYPE_DOUBLE: Byte = 7
  val TYPE_DATETIME: Byte = 8
  val TYPE_TUPLE: Byte = 10
  val TYPE_REF: Byte = 11
  val TYPE_COLLECTION: Byte = 12
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

  def writeLong(value: Long, buf: Array[Byte], offset: Int) = {
    buf(offset) = (value >> 56).asInstanceOf[Byte]
    buf(offset + 1) = (value >> 48).asInstanceOf[Byte]
    buf(offset + 2) = (value >> 40).asInstanceOf[Byte]
    buf(offset + 3) = (value >> 32).asInstanceOf[Byte]
    buf(offset + 4) = (value >> 24).asInstanceOf[Byte]
    buf(offset + 5) = (value >> 16).asInstanceOf[Byte]
    buf(offset + 6) = (value >> 8).asInstanceOf[Byte]
    buf(offset + 7) = value.asInstanceOf[Byte]
  }

  def readLong(buf: Array[Byte], offset: Int) = {
    ((buf(offset) & 0xffL) << 56) |
      ((buf(offset + 1) & 0xffL) << 48) |
      ((buf(offset + 2) & 0xffL) << 40) |
      ((buf(offset + 3) & 0xffL) << 32) |
      ((buf(offset + 4) & 0xffL) << 24) |
      ((buf(offset + 5) & 0xffL) << 16) |
      ((buf(offset + 6) & 0xffL) << 8) |
      (buf(offset + 7) & 0xffL)
  }

  implicit object LongPacker extends FixedLengthPacker[Long](9) {
    def pack(value: Long, buf: Array[Byte], offset: Int) = {
      require(buf.length >= offset + 9, "buffer too small")
      buf(offset) = TYPE_LONG
      writeLong(value, buf, offset + 1)
    }

    def unpack(buf: Array[Byte], offset: Int, length: Int) = {
      require(buf(offset) == TYPE_LONG, "not a Long")
      require(buf.length >= offset + 9 && length == 9, "invalid length")
      readLong(buf, offset + 1)
    }
  }

  implicit object DoublePacker extends FixedLengthPacker[Double](9) {
    def pack(value: Double, buf: Array[Byte], offset: Int) = {
      require(buf.length >= offset + 9, "buffer too small")
      buf(offset) = TYPE_DOUBLE
      val bits = java.lang.Double.doubleToLongBits(value)
      writeLong(bits, buf, offset + 1)
    }

    def unpack(buf: Array[Byte], offset: Int, length: Int) = {
      require(buf(offset) == TYPE_DOUBLE, "not a Double")
      require(buf.length >= offset + 9 && length == 9, "invalid length")
      val bits = readLong(buf, offset + 1)
      java.lang.Double.longBitsToDouble(bits)
    }
  }

  implicit object BooleanPacker extends FixedLengthPacker[Boolean](1) {
    def pack(value: Boolean, buf: Array[Byte], offset: Int) = {
      require(buf.length > offset, "buffer too small")
      buf(offset) = if (value) TYPE_TRUE else TYPE_FALSE
    }

    def unpack(buf: Array[Byte], offset: Int, length: Int) = {
      buf(offset) match {
        case TYPE_TRUE => true
        case TYPE_FALSE => false
        case _ => throw new IllegalArgumentException("not a Boolean")
      }
    }
  }

  val emptyByteArray = new Array[Byte](0)

  abstract class RawPacker[T](typeSign: Byte) extends Packer[T] {
    def pack(value: T): Array[Byte]

    def unpack(buf: Array[Byte]): T

    def pack(value: T, buf: Array[Byte], offset: Int) = {
      val arr = pack(value)
      require(buf.length >= offset + arr.length + 2, "buffer too small")
      require(buf.length < 256, "value too large")
      buf(offset) = typeSign
      buf(offset + 1) = arr.length.asInstanceOf[Byte]
      System.arraycopy(arr, 0, buf, offset + 2, arr.length)
    }

    def packLength(value: T): Int = {
      pack(value).length + 2
    }

    def unpack(buf: Array[Byte], offset: Int, length: Int) = {
      require(buf(offset) == typeSign, "not a byte array")
      require(buf(offset + 1) == length - 2, "invalid length")
      val arr =
        if (length > 2) {
          val arr = new Array[Byte](length - 2)
          System.arraycopy(buf, offset + 2, arr, 0, arr.length)
          arr
        } else
          emptyByteArray
      unpack(arr)
    }

    def unpackLength(buf: Array[Byte], offset: Int): Int = {
      buf(offset + 1) + 2
    }
  }

  implicit object ByteArrayPacker extends RawPacker[Array[Byte]](TYPE_RAW) {
    def pack(value: Array[Byte]) = value

    def unpack(buf: Array[Byte]) = buf
  }

  implicit object DateTimePacker extends FixedLengthPacker[org.joda.time.DateTime](9) {
    def pack(value: org.joda.time.DateTime, buf: Array[Byte], offset: Int) = {
      require(buf.length >= offset + 9, "buffer too small")
      buf(offset) = TYPE_DATETIME
      writeLong(value.getMillis(), buf, offset + 1)
    }

    def unpack(buf: Array[Byte], offset: Int, length: Int) = {
      require(buf(offset) == TYPE_DATETIME, "not a DateTime")
      require(buf.length >= offset + 9 && length == 9, "invalid length")
      val bits = readLong(buf, offset + 1)
      new org.joda.time.DateTime(bits)
    }
  }

  val refPacker = new FixedLengthPacker[Ref[_]](9) {
    override def pack(value: Ref[_], buf: Array[Byte], offset: Int): Unit = {
      buf(offset) = TYPE_REF
      writeLong(value.id, buf, offset + 1)
    }

    override def unpack(buf: Array[Byte], offset: Int, length: Int): Ref[_] = {
      require(buf(offset) == TYPE_REF, "not a ref")
      require(buf.length >= offset + 9 && length == 9, "invalid length")
      val id = readLong(buf, offset + 1)
      if (id != 0) RefSome(id) else RefEmpty
    }
  }

  implicit def ref[T <: Entity]: Packer[Ref[T]] = refPacker.asInstanceOf[Packer[Ref[T]]]

  implicit def collection[T <: Entity]: Packer[KeyCollection[T]] =
    new RawPacker[KeyCollection[T]](TYPE_COLLECTION) {
      def pack(value: KeyCollection[T]) = value.asInstanceOf[KeyCollectionImpl[T]].key

      def unpack(buf: Array[Byte]) = new KeyCollectionImpl[T](buf)
    }
}