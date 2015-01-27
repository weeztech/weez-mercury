package com.weez.mercury.common

sealed trait RangeBound[+A] {
  private[common] def map[B](f: this.type => B): RangeBound[B]
}

sealed trait Range[+A] extends DBRange {
  def map[B: Packer](f: RangeBound[A] => B): Range[B]
}

trait DBRange {
  def keyStart: Array[Byte]

  def keyEnd: Array[Byte]
}

object Range {

  sealed trait InfinitBound extends RangeBound[Nothing] {
    def map[B](f: this.type => B) = Include(f(this))
  }

  sealed trait FinitBound[A] extends RangeBound[A]

  case object Min extends InfinitBound

  case object Max extends InfinitBound

  case class Include[A](value: A) extends FinitBound[A] {
    def map[B](f: this.type => B) = Include(f(this))
  }

  case class Exclude[A](value: A) extends FinitBound[A] {
    def map[B](f: this.type => B) = Exclude(f(this))
  }

  case object All extends Range[Nothing] {
    def keyStart = ByteArrayMin

    def keyEnd = ByteArrayMax

    def map[B: Packer](f: RangeBound[Nothing] => B) = BoundaryRange(Min.map(f), Max.map(f))
  }

  case object Empty extends Range[Nothing] {
    def keyStart = ByteArrayMax

    def keyEnd = ByteArrayMin

    def map[B: Packer](f: RangeBound[Nothing] => B) = this
  }

  case class SingleValueRange[A](value: A)(implicit p: Packer[A]) extends Range[A] {
    def keyStart = p(value)

    def keyEnd = keyStart

    def map[B: Packer](f: RangeBound[A] => B) = SingleValueRange(f(Include(value)))
  }

  case class BoundaryRange[A](start: RangeBound[A], end: RangeBound[A])(implicit p: Packer[A]) extends Range[A] {

    import java.util.Arrays

    def keyStart = start match {
      case Min => ByteArrayMin
      case Max => ByteArrayMax
      case Include(x) => p(x)
      case Exclude(x) =>
        val arr = p(x)
        Arrays.copyOf(arr, arr.length + 1)
    }

    def keyEnd = end match {
      case Min => ByteArrayMin
      case Max => ByteArrayMax
      case Include(x) =>
        val arr = p(x)
        Arrays.copyOf(arr, arr.length + 1)
      case Exclude(x) => p(x)
    }

    def map[B: Packer](f: RangeBound[A] => B) = BoundaryRange(start.map(f), end.map(f))
  }

  case class PrefixRange[A](prefix: A)(implicit p: Packer[A]) extends Range[A] {
    def keyStart = p(prefix)

    def keyEnd = {
      val arr = p(prefix)
      var i = arr.length - 1
      var c = 1
      while (i >= 0 && c > 0) {
        c += (arr(i) & 0xff)
        arr(i) = c.asInstanceOf[Byte]
        c >>= 8
        i -= 1
      }
      if (c > 0) ByteArrayMax else arr
    }

    def map[B: Packer](f: RangeBound[A] => B) = PrefixRange(f(Include(prefix)))
  }

  class ValueHelper[T](val value: T) extends AnyVal {
    def +-+(end: T)(implicit p: Packer[T]) = BoundaryRange(Include(value), Include(end))

    def +--(end: T)(implicit p: Packer[T]) = BoundaryRange(Include(value), Exclude(end))

    def --+(end: T)(implicit p: Packer[T]) = BoundaryRange(Exclude(value), Include(end))

    def ---(end: T)(implicit p: Packer[T]) = BoundaryRange(Exclude(value), Exclude(end))

    def asMax(implicit p: Packer[T]) = BoundaryRange[T](Min, Include(value))

    def asMin(implicit p: Packer[T]) = BoundaryRange[T](Include(value), Max)

    def asPrefix(implicit p: Packer[T]) = PrefixRange(value)
  }

  val ByteArrayMin = new Array[Byte](0)
  val ByteArrayMax = new Array[Byte](0)

  @inline final def isMin(arr: Array[Byte]) = arr eq ByteArrayMin

  @inline final def isMax(arr: Array[Byte]) = arr eq ByteArrayMax

  object ByteArrayOrdering extends Ordering[Array[Byte]] {
    def compare(x: Array[Byte], y: Array[Byte]) = {
      if (x eq y)
        0
      else if ((x eq ByteArrayMin) || (y eq ByteArrayMax))
        -1
      else if ((x eq ByteArrayMax) || (y eq ByteArrayMin))
        1
      else
        Util.compareUInt8s(x, y)
    }
  }

}

object RangeBound {

  import Range._

  val TYPE_MIN: Byte = 0
  val TYPE_MAX: Byte = -1

  class RangeBoundPacker[T](implicit packer: Packer[T]) extends Packer[RangeBound[T]] {
    def pack(value: RangeBound[T], buf: Array[Byte], offset: Int) = {
      value match {
        case Min =>
          buf(offset) = TYPE_MIN
          offset + 1
        case Max =>
          buf(offset) = TYPE_MAX
          offset + 1
        case Include(x) => packer.pack(x, buf, offset)
        case Exclude(x) => packer.pack(x, buf, offset)
      }
    }

    def packLength(value: RangeBound[T]) = {
      value match {
        case Min | Max => 1
        case Include(x) => packer.packLength(x)
        case Exclude(x) => packer.packLength(x)
      }
    }

    def unpack(buf: Array[Byte], offset: Int, length: Int): RangeBound[T] = {
      buf(offset) match {
        case TYPE_MIN => Min
        case TYPE_MAX => Max
        case _ => Include(packer.unpack(buf, offset, length))
      }
    }

    def unpackLength(buf: Array[Byte], offset: Int) = {
      buf(offset) match {
        case TYPE_MIN | TYPE_MAX => 1
        case _ => packer.unpackLength(buf, offset)
      }
    }
  }

  implicit def rangeBoundPacker[T: Packer]: Packer[RangeBound[T]] = new RangeBoundPacker[T]

  implicit val nothingPacker = rangeBoundPacker[Nothing]
}

trait RangeImplicits {

  import scala.language.implicitConversions

  val Min = Range.Min
  val Max = Range.Max

  implicit def value2range[T: Packer](value: T): Range[T] = Range.SingleValueRange(value)

  implicit def value2helper[T](value: T): Range.ValueHelper[T] = new Range.ValueHelper(value)
}

