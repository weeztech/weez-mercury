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
  def get[K, V](key: K)(implicit pk: Packer[K], pv: Packer[V]): V

  def newCursor[K, V](implicit pk: Packer[K], pv: Packer[V]): DBCursor[K, V]

  def put[K, V](key: K, value: V)(implicit pk: Packer[K], pv: Packer[V]): Unit
}

trait DBCursor[K, V] extends Iterator[(K, V)] {
  def seek(key: K): Boolean

  def close(): Unit
}

class KeyCollectionImpl[T <: Entity](val key: Array[Byte]) extends KeyCollection[T] {

  def apply()(implicit db: DBSessionQueryable): Cursor[T] = {
    ???
  }

  def update(id: Long, value: T)(implicit db: DBSessionUpdatable) = {
    import Packer._
    ???
  }

  def apply(id: Long)(implicit db: DBSessionQueryable): Option[T] = ???

  def defUniqueIndex[K: Packer](name: String, keyGetter: T => K): UniqueIndex[K, T] = ???
}

case class RefSome[T <: Entity](key: Array[Byte]) extends Ref[T] {
  def isEmpty = false

  def apply()(implicit db: DBSessionQueryable): T = ???
}

case object RefEmpty extends Ref[Nothing] {
  def isEmpty = true

  def apply()(implicit db: DBSessionQueryable) = ???
}
