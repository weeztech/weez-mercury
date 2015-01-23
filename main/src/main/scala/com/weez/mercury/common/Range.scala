package com.weez.mercury.common

sealed trait RangeBound[T]

sealed trait Range[T]

object Range {
  def all[T]: Range[T] = ???

  case object Empty extends Range[Nothing]

  case class BoundaryRange[T](start: RangeBound[T], end: RangeBound[T]) extends Range[T]

  case object Min extends RangeBound[Nothing]

  case object Max extends RangeBound[Nothing]

  case class Include[T](value: T) extends RangeBound[T]

  case class Exclude[T](value: T) extends RangeBound[T]


  class ValueHepler[T](val value: T) extends AnyVal {
    def +-+(end: T) = Range.BoundaryRange(Range.Include(value), Range.Include(end))

    def +--(end: T) = Range.BoundaryRange(Range.Include(value), Range.Exclude(end))

    def --+(end: T) = Range.BoundaryRange(Range.Exclude(value), Range.Include(end))

    def ---(end: T) = Range.BoundaryRange(Range.Exclude(value), Range.Exclude(end))

    def asMax = Range.BoundaryRange(Min, Range.Include(value))

    def asMin = Range.BoundaryRange(Range.Include(value), Max)
  }

}

trait RangeImplicits {

  import scala.language.implicitConversions

  val Min = Range.Min
  val Max = Range.Max

  implicit def imp_equal[T](value: T): Range[T] = Range.BoundaryRange(Range.Include(value), Range.Include(value))

  implicit def impl_helper[T](value: T): Range.ValueHepler[T] = new Range.ValueHepler(value)
}