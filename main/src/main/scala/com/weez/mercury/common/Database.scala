package com.weez.mercury.common

trait DBSessionQueryable {
  implicit val dbSession = this

  def get[K, V](key: K): V

  def get[K, V](start: K, end: K): Cursor[V]
}

trait DBSessionUpdatable extends DBSessionQueryable {
  override implicit val dbSession = this

  def put[K, V](key: K, value: V): Unit
}

trait Cursor[T] extends Iterator[T]

trait IndexBase[K, V] {
  def apply()(implicit db: DBSessionQueryable): Cursor[V]

  def apply(start: K, end: K, excludeStart: Boolean = false, excludeEnd: Boolean = false)(implicit db: DBSessionQueryable): Cursor[V]
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

  val id = column[Long]("id")

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

