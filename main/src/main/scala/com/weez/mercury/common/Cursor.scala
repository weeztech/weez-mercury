package com.weez.mercury.common

trait Cursor[+V] extends AutoCloseable {
  self =>

  import scala.language.higherKinds
  import scala.collection.generic.CanBuildFrom
  import scala.annotation.unchecked.{uncheckedVariance => uV}

  def next(): Unit

  def isValid: Boolean

  def value: V


  def foreach[U](f: V => U) = {
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

  def slice(from: Int, until: Int): Cursor[V] = {
    drop(from)
    new Cursor[V] {
      var remain = until - from

      def isValid = {
        if (remain > 0) {
          self.isValid
        } else {
          self.close()
          false
        }
      }

      def value = self.value

      def next() = {
        if (remain > 0) {
          remain -= 1
          if (remain < 0) {
            self.close()
          } else {
            self.next()
          }
        }
      }

      def close() = self.close()
    }
  }

  @inline final def take(n: Int): Cursor[V] = slice(0, n)


  @inline final def drop(n: Int): Cursor[V] = {
    var count = n
    while (count > 0) {
      next()
      count -= 1
    }
    this
  }

  @inline final def map[B](f: V => B): Cursor[B] =
    new Cursor[B] {
      def isValid = self.isValid

      def value = f(self.value)

      def next() = self.next()

      def close() = self.close()
    }

  def filter(f: V => Boolean): Cursor[V] =
    new Cursor[V] {
      def next() = {
        self.next()
        while (self.isValid && !f(self.value))
          self.next()
      }

      def isValid = self.isValid

      def value = self.value

      def close() = self.close()
    }

  def toSeq: Seq[V] = to[Seq]

  def toList: List[V] = to[List]

  def to[Col[_]](implicit cbf: CanBuildFrom[Nothing, V, Col[V@uV]]): Col[V@uV] = {
    val b = cbf()
    while (isValid) {
      b += value
      next()
    }
    b.result()
  }
}

object Cursor {
  @inline final def apply[V: Packer](range: DBRange, forward: Boolean)(implicit db: DBSessionQueryable): Cursor[V] = new CursorImpl[V](range, forward)

  @inline final def raw[V: Packer](range: DBRange, forward: Boolean)(implicit db: DBSessionQueryable) = new CursorImpl[V](range, forward)

  class CursorImpl[V](range: DBRange, forward: Boolean)(implicit db: DBSessionQueryable, pv: Packer[V]) extends Cursor[V] {
    self =>

    import com.weez.mercury.debug._
    import Range.{ByteArrayOrdering => O}

    val step = if (forward) 1 else -1
    val rangeStart = range.keyStart
    val rangeEnd = range.keyEnd
    var dbCursor: DBCursor =
      if (O.compare(rangeStart, rangeEnd) < 0) {
        val c = db.newCursor()
        if (forward)
          c.seek(rangeStart)
        else {
          c.seek(rangeEnd)
          if (c.isValid) c.next(-1)
        }
        c
      } else null

    var _value: V = _
    var _key: Array[Byte] = _
    final val UPDATE_BIT_KEY = 1
    final val UPDATE_BIT_VALUE = 2
    var updates: Int = 0

    def isValid: Boolean = {
      dbCursor != null && {
        val valid =
          dbCursor.isValid && {
            if (forward)
              O.compare(dbCursor.key(), rangeEnd) < 0
            else
              O.compare(dbCursor.key(), rangeStart) >= 0
          }
        if (!valid) close()
        valid
      }
    }

    def value: V = {
      if ((updates & UPDATE_BIT_VALUE) == 0) {
        _value = pv.unapply(dbCursor.value())
        updates |= UPDATE_BIT_VALUE
      }
      _value
    }

    def key = {
      if ((updates & UPDATE_BIT_KEY) == 0) {
        _key = dbCursor.key()
        updates |= UPDATE_BIT_KEY
      }
      _key
    }

    def next() = {
      if (dbCursor != null) {
        dbCursor.next(step)
        updates = 0
      }
    }

    def close() = {
      if (dbCursor != null) {
        dbCursor.close()
        dbCursor = null
        updates = 0
      }
    }

    @inline final def mapWithKey[B](f: (Array[Byte], V) => B): Cursor[B] =
      new Cursor[B] {
        def isValid = self.isValid

        def value = f(self.key, self.value)

        def next() = self.next()

        def close() = self.close()
      }
  }

}

