package com.weez.mercury.common

import akka.event.LoggingAdapter

trait Database {
  def withQuery(log: LoggingAdapter)(f: DBSessionQueryable => Unit): Unit

  def withUpdate(log: LoggingAdapter)(f: DBSessionUpdatable => Unit): Unit
}

trait DBSessionQueryable {
  def get[K: Packer, V: Packer](key: K): V

  def get[K: Packer, V: Packer](start: K, end: K): Cursor[V]
}

trait DBSessionUpdatable extends DBSessionQueryable {
  def put[K: Packer, V: Packer](key: K, value: V): Unit
}

trait Cursor[T] extends Iterator[T] {
  def close(): Unit
}

trait IndexBase[K, V] {
  def apply(start: K, end: K)(implicit db: DBSessionQueryable): Cursor[V]
}

trait Index[K, V] extends IndexBase[K, V]

trait UniqueIndex[K, V] extends IndexBase[K, V] {
  def apply(key: K)(implicit db: DBSessionQueryable): Option[V]
}

trait Ref[T] {
  def isEmpty: Boolean

  def apply()(implicit db: DBSessionQueryable): T
}

trait DBObjectType[T] {

  def nameInDB: String

  def column[T](name: String) = Column[T](name)

  case class Column[T](name: String)

}

trait KeyCollection[T] {
  def apply(id: Long)(implicit db: DBSessionQueryable): Option[T]

  def defUniqueIndex[S <: DBObjectType[T], A](name: String, column: S#Column[A]): UniqueIndex[A, T]
}

trait RootCollection[T] extends KeyCollection[T] {
  def apply(id: Long)(implicit db: DBSessionQueryable): Option[T] = ???

  def defUniqueIndex[S <: DBObjectType[T], A](name: String, column: S#Column[A]): UniqueIndex[A, T] = ???
}

trait Packer[T] {
  def apply(value: T): Array[Byte]

  def unapply(buf: Array[Byte]): T
}
