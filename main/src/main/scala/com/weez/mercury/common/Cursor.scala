package com.weez.mercury.common

trait Cursor[+T] extends AutoCloseable {
  self =>

  import scala.language.higherKinds
  import scala.collection.generic.CanBuildFrom
  import scala.annotation.unchecked.{uncheckedVariance => uV}

  def next(): Unit

  def isValid: Boolean

  def value: T

  def foreach[U](f: T => U) {
    while (isValid) {
      f(value)
      next()
    }
  }

  def size: Int = {
    var count = 0
    while (isValid) {
      count += 1
      next()
    }
    count
  }

  def slice(from: Int, until: Int): Cursor[T] = {
    var count = from
    while (count > 0) {
      next()
      count -= 1
    }
    new Cursor[T] {
      var remain = until - from

      def isValid = remain > 0 && self.isValid

      def value = self.value

      def next() = {
        if (remain > 0) {
          remain -= 1
          self.next()
        }
      }

      def close() = self.close()
    }
  }

  @inline final def take(n: Int): Cursor[T] = slice(0, n)


  @inline final def drop(n: Int): Cursor[T] = {
    var count = n
    while (count > 0) {
      next()
      count -= 1
    }
    this
  }

  def map[B](f: T => B): Cursor[B] =
    new Cursor[B] {
      def isValid = self.isValid

      def value = f(self.value)

      def next() = self.next()

      def close() = self.close()
    }

  def filter(f: T => Boolean): Cursor[T] =
    new Cursor[T] {
      def next() = {
        self.next()
        while (self.isValid && !f(self.value))
          self.next()
      }

      def isValid = self.isValid

      def value = self.value

      def close() = self.close()
    }

  def toSeq: Seq[T] = to[Seq]

  def toList: List[T] = to[List]

  def to[Col[_]](implicit cbf: CanBuildFrom[Nothing, T, Col[T@uV]]): Col[T@uV] = {
    val b = cbf()
    while (isValid) {
      b += value
      next()
    }
    b.result()
  }
}
