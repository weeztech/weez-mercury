package com.weez.mercury.common

import akka.event.LoggingAdapter

trait Database {
  def createSession(): DBSession

  def close(): Unit
}

trait DatabaseFactory {
  def createNew(path: String): Database

  def open(path: String): Database

  def delete(path: String): Unit
}

trait DBSession {
  def newTransaction(log: LoggingAdapter): DBTransaction

  def close(): Unit
}

trait DBTransaction {
  def get(key: Array[Byte]): Array[Byte]

  def newCursor(): DBCursor

  def put(key: Array[Byte], value: Array[Byte]): Unit

  def exists(key: Array[Byte]): Boolean

  def del(key: Array[Byte]): Unit

  def commit(): Unit

  def close()
}

trait DBCursor {

  def seek(key: Array[Byte]): Boolean

  def next(n: Int): Boolean

  def isValid: Boolean

  def key(): Array[Byte]

  def value(): Array[Byte]

  def close(): Unit
}

class KeyCollectionImpl[T <: Entity](val key: Array[Byte]) extends KeyCollection[T] {

  def update(value: T)(implicit db: DBSessionUpdatable): Unit = ???

  def apply(start: Option[Long], end: Option[Long], excludeStart: Boolean, excludeEnd: Boolean)(implicit db: DBSessionQueryable): Cursor[T] = ???

  def delete(key: Long)(implicit db: DBSessionUpdatable): Unit = ???

  def update(id: Long, value: T)(implicit db: DBSessionUpdatable) = {
    import Packer._
    ???
  }

  def apply(id: Long)(implicit db: DBSessionQueryable): Option[T] = ???

  def defUniqueIndex[K: Packer](name: String, keyGetter: T => K): UniqueIndex[K, T] = ???
}

case class RefSome[T <: Entity](id: Long) extends Ref[T] {

  def apply()(implicit db: DBSessionQueryable): T = EntityCollections.getEntity(id)
}

case object RefEmpty extends Ref[Nothing] {
  def id = 0L

  def apply()(implicit db: DBSessionQueryable) = throw new NoSuchElementException("RefEmpty()")
}
