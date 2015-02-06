package com.weez.mercury.common

import scala.language.implicitConversions

/**
 * do NOT implement this interfact directly,
 * use Packer.map/imp_tuple/caseClass or @packable instead.
 * @tparam T
 */
trait Packer[T] {
  def apply(value: T): Array[Byte] = {
    val buf = new Array[Byte](packLength(value))
    pack(value, buf, 0)
    buf
  }

  def unapply(buf: Array[Byte]): T = unpack(buf, 0, buf.length)

  def pack(value: T, buf: Array[Byte], offset: Int): Int

  def packLength(value: T): Int

  def unpack(buf: Array[Byte], offset: Int, length: Int): T

  def unpackLength(buf: Array[Byte], offset: Int): Int
}

import com.weez.mercury.macros._

@tuplePackers
@caseClassPackers
object Packer extends PackerMacros with CollectionPackers {
  val TYPE_STRING: Byte = 11
  val TYPE_UINT32: Byte = 12
  val TYPE_UINT64: Byte = 13
  val TYPE_FALSE: Byte = 14
  val TYPE_TRUE: Byte = 15
  val TYPE_RAW: Byte = 16
  val TYPE_TUPLE: Byte = 17
  val TYPE_END: Byte = 0

  @inline def of[T](implicit p: Packer[T]) = p

  def pack[T](value: T)(implicit packer: Packer[T]) = packer(value)

  def unpack[T](buf: Array[Byte])(implicit packer: Packer[T]) = packer.unapply(buf)

  abstract class FixedLengthPacker[T](val length: Int) extends Packer[T] {
    def packLength(value: T) = length

    def unpackLength(buf: Array[Byte], offset: Int) = length
  }

  val charset = java.nio.charset.Charset.forName("UTF-8")

  implicit object StringPacker extends Packer[String] {
    def pack(value: String, buf: Array[Byte], offset: Int) = {
      val arr = value.getBytes(charset)
      require(buf.length > offset + arr.length + 1, "buffer too small")
      buf(offset) = TYPE_STRING
      buf(offset + arr.length + 1) = TYPE_END
      System.arraycopy(arr, 0, buf, offset + 1, arr.length)
      offset + arr.length + 2
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
      buf(offset) = TYPE_UINT32
      buf(offset + 1) = (value >> 24).asInstanceOf[Byte]
      buf(offset + 2) = (value >> 16).asInstanceOf[Byte]
      buf(offset + 3) = (value >> 8).asInstanceOf[Byte]
      buf(offset + 4) = value.asInstanceOf[Byte]
      offset + 5
    }

    def unpack(buf: Array[Byte], offset: Int, length: Int) = {
      require(buf(offset) == TYPE_UINT32, "not a Int")
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
      buf(offset) = TYPE_UINT64
      buf(offset + 1) = (value >> 56).asInstanceOf[Byte]
      buf(offset + 2) = (value >> 48).asInstanceOf[Byte]
      buf(offset + 3) = (value >> 40).asInstanceOf[Byte]
      buf(offset + 4) = (value >> 32).asInstanceOf[Byte]
      buf(offset + 5) = (value >> 24).asInstanceOf[Byte]
      buf(offset + 6) = (value >> 16).asInstanceOf[Byte]
      buf(offset + 7) = (value >> 8).asInstanceOf[Byte]
      buf(offset + 8) = value.asInstanceOf[Byte]
      offset + 9
    }

    def unpack(buf: Array[Byte], offset: Int, length: Int) = {
      require(buf(offset) == TYPE_UINT64, "not a Long")
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

  implicit object BooleanPacker extends FixedLengthPacker[Boolean](1) {
    def pack(value: Boolean, buf: Array[Byte], offset: Int) = {
      require(buf.length > offset, "buffer too small")
      buf(offset) = if (value) TYPE_TRUE else TYPE_FALSE
      offset + 1
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

  implicit object ByteArrayPacker extends Packer[Array[Byte]] {
    def pack(value: Array[Byte], buf: Array[Byte], offset: Int) = {
      require(buf.length >= offset + value.length + 2, "buffer too small")
      require(buf.length < 256, "value too large")
      buf(offset) = TYPE_RAW
      buf(offset + 1) = value.length.asInstanceOf[Byte]
      System.arraycopy(value, 0, buf, offset + 2, value.length)
      offset + value.length + 2
    }

    def packLength(value: Array[Byte]): Int = {
      value.length + 2
    }

    def unpack(buf: Array[Byte], offset: Int, length: Int) = {
      require(buf(offset) == TYPE_RAW, "not a byte array")
      require((buf(offset + 1) & 0xff) == length - 2, "invalid length")
      val arr =
        if (length > 2) {
          val arr = new Array[Byte](length - 2)
          System.arraycopy(buf, offset + 2, arr, 0, arr.length)
          arr
        } else
          emptyByteArray
      arr
    }

    def unpackLength(buf: Array[Byte], offset: Int): Int = {
      buf(offset + 1) + 2
    }
  }


  sealed trait MappedPacker[A, B] extends Packer[A] {
    def underlying: Packer[B]

    def mapTo: A => B

    def mapFrom: B => A

    def pack(value: A, buf: Array[Byte], offset: Int) = underlying.pack(mapTo(value), buf, offset)

    def packLength(value: A) = underlying.packLength(mapTo(value))

    def unpack(buf: Array[Byte], offset: Int, length: Int) = mapFrom(underlying.unpack(buf, offset, length))

    def unpackLength(buf: Array[Byte], offset: Int) = underlying.unpackLength(buf, offset)
  }

  def map[A, B](to: A => B, from: B => A)(implicit _underlying: Packer[B]): Packer[A] = {
    new MappedPacker[A, B] {
      def underlying = _underlying

      def mapTo = to

      def mapFrom = from
    }
  }

  def getLengthByType(buf: Array[Byte], offset: Int): Int = {
    buf(offset) match {
      case TYPE_STRING => StringPacker.unpackLength(buf, offset)
      case TYPE_UINT32 => IntPacker.length
      case TYPE_UINT64 => LongPacker.length
      case TYPE_TRUE => BooleanPacker.length
      case TYPE_FALSE => BooleanPacker.length
      case TYPE_RAW => ByteArrayPacker.unpackLength(buf, offset)
      case TYPE_TUPLE =>
        var end = offset + 1
        while (buf(end) != TYPE_END)
          end += getLengthByType(buf, end)
        end + 1 - offset
    }
  }

  implicit val DoublePacker = map[Double, Long](java.lang.Double.doubleToLongBits, java.lang.Double.longBitsToDouble)

  implicit val DateTimePacker = map[org.joda.time.DateTime, Long](_.getMillis, new org.joda.time.DateTime(_))

  implicit def optionPacker[T](implicit p: Packer[T]): Packer[Option[T]] =
    new Packer[Option[T]] {
      def pack(value: Option[T], buf: Array[Byte], offset: Int) = {
        buf(offset) = TYPE_TUPLE
        val end =
          value match {
            case Some(x) => p.pack(x, buf, offset + 1)
            case None => offset + 1
          }
        buf(end) = TYPE_END
        end + 1
      }

      def packLength(value: Option[T]) = {
        value match {
          case Some(x) => 2 + p.packLength(x)
          case None => 2
        }
      }

      def unpack(buf: Array[Byte], offset: Int, length: Int) = {
        require(buf(offset) == TYPE_TUPLE, "not a option")
        if (buf(offset + 1) == TYPE_END)
          None
        else {
          val len = p.unpackLength(buf, offset + 1)
          require(buf(offset + 1 + len) == TYPE_END, "invalid length")
          Some(p.unpack(buf, offset + 1, len))
        }
      }

      def unpackLength(buf: Array[Byte], offset: Int) = {
        if (buf(offset + 1) == TYPE_END) 2 else 2 + p.unpackLength(buf, offset + 1)
      }
    }

  val refPacker = map[Ref[_], Long](_.id, id => if (id != 0) RefSome(id) else RefEmpty)

  implicit def ref[T <: Entity]: Packer[Ref[T]] = refPacker.asInstanceOf[Packer[Ref[T]]]

}
