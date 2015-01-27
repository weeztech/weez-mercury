package com.weez.mercury.common

trait Cursor[K, +V] extends AutoCloseable {
  self =>

  import scala.language.higherKinds
  import scala.collection.generic.CanBuildFrom
  import scala.annotation.unchecked.{uncheckedVariance => uV}

  def next(): Unit

  def isValid: Boolean

  def value: V

  def key: K

  def foreach[U](f: V => U) {
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

  def slice(from: Int, until: Int): Cursor[K, V] = {
    drop(from)
    new Cursor[K, V] {
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

      def key = self.key

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

  @inline final def take(n: Int): Cursor[K, V] = slice(0, n)


  @inline final def drop(n: Int): Cursor[K, V] = {
    var count = n
    while (count > 0) {
      next()
      count -= 1
    }
    this
  }

  def mapKV[KB, B](kf: K => KB, f: V => B): Cursor[KB, B] =
    new Cursor[KB, B] {
      def isValid = self.isValid

      def value = f(self.value)

      def key = kf(self.key)

      def next() = self.next()

      def close() = self.close()
    }

  def map[B](f: V => B): Cursor[K, B] = mapKV(k => k, f)

  def filter(f: V => Boolean): Cursor[K, V] =
    new Cursor[K, V] {
      def next() = {
        self.next()
        while (self.isValid && !f(self.value))
          self.next()
      }

      def isValid = self.isValid

      def value = self.value

      def key = self.key

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
  @inline final def apply[K: Packer, V: Packer](range: DBRange, forward: Boolean)(implicit db: DBSessionQueryable) = new CursorImpl[K, V](range, forward)

  class CursorImpl[K, V](range: DBRange, forward: Boolean)(implicit db: DBSessionQueryable, pk: Packer[K], pv: Packer[V]) extends Cursor[K, V] {

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
    var _key: K = _
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
        _value = try {
          pv.unapply(dbCursor.value())
        } catch {
          case ex: IllegalArgumentException =>
            println("range : " + range)
            println("key   : " + PackerDebug.show(dbCursor.key()))
            println("value : " + PackerDebug.show(dbCursor.value()))
            throw ex
        }
        updates |= UPDATE_BIT_VALUE
      }
      _value
    }

    def key: K = {
      if ((updates & UPDATE_BIT_KEY) == 0) {
        _key = try {
          pk.unapply(dbCursor.key())
        } catch {
          case ex: IllegalArgumentException =>
            println("range : " + range)
            println("key   : " + PackerDebug.show(dbCursor.key()))
            println("value : " + PackerDebug.show(dbCursor.value()))
            throw ex
        }
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
  }

}

