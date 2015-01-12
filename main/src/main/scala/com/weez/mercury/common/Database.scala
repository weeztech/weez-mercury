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

  def get[K, V](start: K, end: K)(implicit pk: Packer[K], pv: Packer[V]): DBCursor[K, V]

  def put[K, V](key: K, value: V)(implicit pk: Packer[K], pv: Packer[V]): Unit
}

trait DBCursor[K, V] extends Iterator[(K, V)] {
  def close(): Unit
}

class KeyCollectionImpl[T](val key: Array[Byte]) extends KeyCollection[T] {

  def apply()(implicit db: DBSessionQueryable): Cursor[T] = {
    ???
  }

  def update(id: Long, value: T)(implicit db: DBSessionUpdatable) = {
    import Packer._
    ???
  }

  def apply(id: Long)(implicit db: DBSessionQueryable): Option[T] = ???

  def defUniqueIndex[S <: DBObjectType[T], A](name: String, column: S#Column[A]): UniqueIndex[A, T] = ???

  def defExtendIndex[S <: DBObjectType[_], A](name: String, column: S#Column[A]): ExtendIndex[A, T] = ???
}

case class RefSome[T](key: Array[Byte]) extends Ref[T] {
  def isEmpty = false

  def apply()(implicit db: DBSessionQueryable): T = ???
}

case object RefEmpty extends Ref[Nothing] {
  def isEmpty = true

  def apply()(implicit db: DBSessionQueryable) = ???
}
