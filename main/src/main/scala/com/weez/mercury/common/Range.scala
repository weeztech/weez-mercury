package com.weez.mercury.common

sealed trait RangeBound[+A] {
  def map[B: Packer](f: A => B): RangeBound[B]

  def toArray: Array[Byte]
}

sealed trait Range[+A] extends DBRange {
  def start: RangeBound[A]

  def end: RangeBound[A]

  def map[B: Packer](f: A => B): Range[B]

  def keyStart = start.toArray

  def keyEnd = end.toArray
}

trait DBRange {
  def keyStart: Array[Byte]

  def keyEnd: Array[Byte]
}

object Range {

  case class Bound[A](value: A)(implicit p: Packer[A]) extends RangeBound[A] {
    def map[B: Packer](f: A => B) = Bound(f(value))

    def toArray = p(value)
  }

  case class Succeed[A](value: A)(implicit p: Packer[A]) extends RangeBound[A] {
    def map[B: Packer](f: A => B) = Succeed(f(value))

    def toArray = succeed(p(value), 0)()

    import scala.annotation.tailrec

    @tailrec
    private def succeed(buf: Array[Byte], offset: Int)(end: Int = buf.length): Array[Byte] = {
      buf(offset) match {
        case Packer.TYPE_STRING =>
          val end = offset + Packer.StringPacker.unpackLength(buf, offset)
          val arr = new Array[Byte](buf.length + 1)
          Array.copy(buf, 0, arr, 0, end - 1)
          Array.copy(buf, end - 1, arr, end, buf.length - end + 1)
          arr(end - 1) = TYPE_MIN
          arr
        case Packer.TYPE_UINT32 =>
          val end = offset + Packer.IntPacker.length
          if (!succeedInPlace(buf, offset + 1, end))
            buf(offset) = TYPE_MAX
          buf
        case Packer.TYPE_UINT64 =>
          val end = offset + Packer.LongPacker.length
          if (!succeedInPlace(buf, offset + 1, end))
            buf(offset) = TYPE_MAX
          buf
        case Packer.TYPE_FALSE =>
          buf(offset) = Packer.TYPE_TRUE
          buf
        case Packer.TYPE_TRUE =>
          buf(offset) = TYPE_MAX
          buf
        case Packer.TYPE_RAW =>
          buf(offset) = TYPE_MAX
          buf
        case Packer.TYPE_TUPLE =>
          // 因为tuple是分段比较的，所以应当在tuple最后一个元素上计算后继。
          if (buf(offset + 1) == Packer.TYPE_END) {
            // 空tuple增加一个元素
            val arr = new Array[Byte](buf.length + 1)
            Array.copy(buf, 0, arr, 0, offset + 1)
            Array.copy(buf, offset + 1, arr, offset + 2, buf.length - offset - 1)
            arr(offset + 1) = TYPE_MIN
            arr
          } else {
            var elemEnd = offset + 1
            while (elemEnd < end - 1) {
              elemEnd = offset + Packer.getLengthByType(buf, elemEnd)
            }
            succeed(buf, elemEnd)(end - 1)
          }
      }
    }

    private def succeedInPlace(buf: Array[Byte], start: Int, end: Int) = {
      var i = end - 1
      var c = 1
      while (c > 0 && i >= start) {
        c += buf(i) & 0xff
        buf(i) = c.asInstanceOf[Byte]
        c >>= 8
        i -= 1
      }
      i >= start
    }
  }

  case class BoundaryRange[A: Packer](start: RangeBound[A], end: RangeBound[A]) extends Range[A] {
    def map[B: Packer](f: A => B) = BoundaryRange(start.map(f), end.map(f))
  }

  class ValueHelper[T](val value: T) extends AnyVal {
    def +-+(end: T)(implicit p: Packer[T]) = BoundaryRange(Bound(value), Succeed(end))

    def +--(end: T)(implicit p: Packer[T]) = BoundaryRange(Bound(value), Bound(end))

    def --+(end: T)(implicit p: Packer[T]) = BoundaryRange(Succeed(value), Succeed(end))

    def ---(end: T)(implicit p: Packer[T]) = BoundaryRange(Succeed(value), Bound(end))

    def +--(end: RangeBound[T])(implicit p: Packer[T]) = BoundaryRange(Bound(value), end)

    def ---(end: RangeBound[T])(implicit p: Packer[T]) = BoundaryRange(Succeed(value), end)

    def asRange(implicit p: Packer[T]) = BoundaryRange(Bound(value), Succeed(value))
  }

  val maxString = "" + Char.MaxValue + Char.MaxValue + Char.MaxValue

  class StringHelper(val value: String) extends AnyVal {
    def prefixed: String = value + maxString
  }

  val TYPE_MIN: Byte = 1
  val TYPE_MAX: Byte = -1

  object ByteArrayOrdering extends Ordering[Array[Byte]] {
    def compare(x: Array[Byte], y: Array[Byte]) = Util.compareUInt8s(x, y)
  }

  import scala.language.implicitConversions

  implicit def value2helper[T](value: T): ValueHelper[T] = new ValueHelper(value)

  implicit def string2helper(value: String): StringHelper = new StringHelper(value)
}

trait RangeImplicits {

  import scala.language.implicitConversions

  implicit def value2helper[T](value: T): Range.ValueHelper[T] = new Range.ValueHelper(value)

  implicit def string2helper(value: String): Range.StringHelper = new Range.StringHelper(value)
}


