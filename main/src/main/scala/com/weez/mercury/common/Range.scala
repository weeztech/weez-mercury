package com.weez.mercury.common

sealed trait RangeBound[+A]

sealed trait Range[+A] extends DBRange {
  def map[B: Packer](f: RangeBound[A] => B): Range[B]
}

trait DBRange {
  def keyStart: Array[Byte]

  def keyEnd: Array[Byte]
}

object Range {

  case object Min extends RangeBound[Nothing]

  case object Max extends RangeBound[Nothing]

  case class Bound[A](value: A) extends RangeBound[A]

  case class Succeed[A](value: A) extends RangeBound[A]

  case class Prefixed(value: String) extends RangeBound[Nothing]

  case object All extends Range[Nothing] {
    val keyStart = Array(TYPE_MIN)

    val keyEnd = Array(TYPE_MAX)

    def map[B: Packer](f: RangeBound[Nothing] => B) = BoundaryRange(Bound(f(Min)), Bound(f(Max)))
  }

  case object Empty extends Range[Nothing] {
    val keyStart = Array(TYPE_MIN)

    val keyEnd = Array(TYPE_MIN)

    def map[B: Packer](f: RangeBound[Nothing] => B) = this
  }

  case class BoundaryRange[A](start: RangeBound[A], end: RangeBound[A])(implicit p: Packer[RangeBound[A]]) extends Range[A] {
    lazy val keyStart = p(start)

    lazy val keyEnd = p(end)

    def map[B: Packer](f: RangeBound[A] => B) = BoundaryRange(Bound(f(start)), Bound(f(end)))
  }

  class ValueHelper[T](val value: T) extends AnyVal {
    def +-+(end: T)(implicit p: Packer[T]) = BoundaryRange(Bound(value), Succeed(end))

    def +--(end: T)(implicit p: Packer[T]) = BoundaryRange(Bound(value), Bound(end))

    def --+(end: T)(implicit p: Packer[T]) = BoundaryRange(Succeed(value), Succeed(end))

    def ---(end: T)(implicit p: Packer[T]) = BoundaryRange(Succeed(value), Bound(end))

    def +--(end: RangeBound[T])(implicit p: Packer[T]) = BoundaryRange(Bound(value), end)

    def ---(end: RangeBound[T])(implicit p: Packer[T]) = BoundaryRange(Succeed(value), end)

    def asMax(implicit p: Packer[T]) = BoundaryRange[T](Min, Bound(value))

    def asMaxInclude(implicit p: Packer[T]) = BoundaryRange[T](Min, Succeed(value))

    def asMin(implicit p: Packer[T]) = BoundaryRange[T](Bound(value), Max)

    def asMinExclude(implicit p: Packer[T]) = BoundaryRange[T](Succeed(value), Max)

    def asRange(implicit p: Packer[T]) = BoundaryRange(Bound(value), Succeed(value))
  }

  class StringHelper(val value: String) extends AnyVal {
    def prefixed = Prefixed(value)

    def asPrefix = BoundaryRange(Bound(value), Prefixed(value))
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

object RangeBound {

  import Range._

  class RangeBoundPacker[T](implicit packer: Packer[T]) extends Packer.RawPacker[RangeBound[T]] {
    def pack(value: RangeBound[T], buf: Array[Byte], offset: Int) = {
      value match {
        case Min =>
          buf(offset) = TYPE_MIN
          offset + 1
        case Max =>
          buf(offset) = TYPE_MAX
          offset + 1
        case Bound(x) =>
          packer.pack(x, buf, offset)
        case Succeed(x) =>
          val end = packer.pack(x, buf, offset)
          buf(offset) match {
            case Packer.TYPE_STRING | Packer.TYPE_TUPLE =>
              buf(end - 1) = TYPE_MIN
              buf(end) = Packer.TYPE_END
              end + 1
            case Packer.TYPE_UINT32 | Packer.TYPE_UINT64 =>
              if (!succeedInPlace(buf, offset + 1, end))
                buf(offset) = TYPE_MAX
              end
            case Packer.TYPE_FALSE =>
              buf(offset) = Packer.TYPE_TRUE
              end
            case Packer.TYPE_TRUE =>
              buf(offset) = TYPE_MAX
              end
            case Packer.TYPE_RAW =>
              if (!succeedInPlace(buf, offset + 2, end))
                buf(offset) = TYPE_MAX
              end
          }
        case Prefixed(x) =>
          val end = packer.asInstanceOf[Packer[String]].pack(x, buf, offset)
          buf(end - 1) = TYPE_MAX
          buf(end) = Packer.TYPE_END
          end + 1
      }
    }

    def succeedInPlace(buf: Array[Byte], start: Int, end: Int) = {
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

    def packLength(value: RangeBound[T]) = {
      value match {
        case Min | Max => 1
        case Bound(x) => packer.packLength(x)
        case Succeed(x) =>
          def getLen[A](packer: Packer[A], v: A): Int = {
            packer match {
              case p: Packer.MappedPacker[A, _] => getLen(p.underlying, p.mapTo(v))
              case p: Packer.FixedLengthPacker[_] => p.packLength(v)
              case p if p eq Packer.ByteArrayPacker => p.packLength(v)
              case p => p.packLength(v) + 1
            }
          }
          getLen(packer, x)
        case Prefixed(x) => Packer.of[String].packLength(x) + 1
      }
    }

    def unpack(buf: Array[Byte], offset: Int, length: Int): RangeBound[T] = ???

    def unpackLength(buf: Array[Byte], offset: Int) = ???
  }

  implicit def rangeBoundPacker[T: Packer]: Packer[RangeBound[T]] = new RangeBoundPacker[T]

  implicit val nothingPacker: Packer[RangeBound[Nothing]] = rangeBoundPacker[Nothing]
}

trait RangeImplicits {

  import scala.language.implicitConversions

  val Min = Range.Min
  val Max = Range.Max
  val Empty = Range.Empty
  val All = Range.All

  implicit def value2helper[T](value: T): Range.ValueHelper[T] = new Range.ValueHelper(value)

  implicit def string2helper(value: String): Range.StringHelper = new Range.StringHelper(value)
}

