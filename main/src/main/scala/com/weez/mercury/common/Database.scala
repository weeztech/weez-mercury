package com.weez.mercury.common

import akka.event.LoggingAdapter

trait Database {
  def createSession(): DBSession

  def close(): Unit
}

object Database {
  val KEY_OBJECT_ID_COUNTER = "object-id-counter"
}

trait DatabaseFactory {
  def createNew(path: String): Database

  def open(path: String): Database

  def delete(path: String): Unit
}

trait DBSession {
  def withTransaction(log: LoggingAdapter)(f: DBTransaction => Unit): Unit

  def close(): Unit
}

trait DBTransaction {
  def get[K, V](key: K)(implicit pk: Packer[K], pv: Packer[V]): Option[V]

  def newCursor[K, V](implicit pk: Packer[K], pv: Packer[V]): DBCursor[K, V]

  def put[K, V](key: K, value: V)(implicit pk: Packer[K], pv: Packer[V]): Unit

  def exists[K](key: K)(implicit pk: Packer[K]): Boolean

  def del[K](key: K)(implicit pk: Packer[K]): Unit
}

trait DBCursor[K, V] extends Iterator[(K, V)] {
  def seek(key: K): Boolean

  def close(): Unit
}

class KeyCollectionImpl[T <: Entity](val key: Array[Byte]) extends KeyCollection[T] {

  override def update(value: T)(implicit db: DBSessionUpdatable): Unit = ???

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
