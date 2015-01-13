package com.weez.mercury.common

import akka.event.LoggingAdapter

trait Context {
  implicit val context: this.type = this

  def log: LoggingAdapter

  def request: ModelObject

  def response: ModelObject

  def complete(response: ModelObject): Unit
}

trait SessionState {
  def session: Session

  def sessionsByPeer(peer: String = session.peer): Seq[Session]
}

trait DBSessionQueryable {
  def get[K: Packer, V: Packer](key: K): Option[V]

  def newCursor[K: Packer, V: Packer]: DBCursor[K, V]
}

trait DBSessionUpdatable extends DBSessionQueryable {
  def put[K: Packer, V: Packer](key: K, value: V): Unit
}

trait Cursor[+T] extends Iterator[T] {
  //override def drop(n: Int): Cursor[T]
  def close(): Unit
}

trait IndexBase[K, V] {
  def apply()(implicit db: DBSessionQueryable): Cursor[V]

  def apply(start: K, end: K, includeStart: Boolean = true, includeEnd: Boolean = false)(implicit db: DBSessionQueryable): Cursor[V]
}

trait Index[K, V] extends IndexBase[K, V]

trait UniqueIndex[K, V] extends IndexBase[K, V] {
  def apply(key: K)(implicit db: DBSessionQueryable): Option[V]
}

trait ExtendIndex[K, V] extends UniqueIndex[K, V]


trait Ref[+T] {
  def isEmpty: Boolean

  def apply()(implicit db: DBSessionQueryable): T
}

trait DBObjectType[T] {
  dbObject =>
  def nameInDB: String

  def column[CT](name: String) = Column[CT](name)

  def extend[ST <: DBObjectType[_]](name: String, source: ST) = Extend[ST](name, source)

  val id = column[Long]("id")

  case class Extend[ST](name: String, source: ST)

  case class Column[CT](name: String) {
    def owner = dbObject
  }

}

trait KeyCollection[T] {
  def apply()(implicit db: DBSessionQueryable): Cursor[T]

  def apply(id: Long)(implicit db: DBSessionQueryable): Option[T]

  def update(id: Long, value: T)(implicit db: DBSessionUpdatable): Unit

  def defUniqueIndex[S <: DBObjectType[T], K: Packer](name: String, column: S#Column[K]): UniqueIndex[K, T]

  def defExtendIndex[S <: DBObjectType[_], K: Packer](name: String, column: S#Column[K]): ExtendIndex[K, T]
}

class CursorImpl[K: Packer, T](prefix: String, dbCursor: DBCursor[(String, K), T]) extends Cursor[T] {

  def next(): T = ???

  def hasNext = dbCursor.hasNext

  def close() = dbCursor.close()
}

abstract class RootCollection[T: Packer] extends KeyCollection[T] {
  rootCollection =>

  def name: String = ???

  val indexes = collection.mutable.Map[String,Any]()


  @inline def apply()(implicit db: DBSessionQueryable): Cursor[T] =
    new CursorImpl[Long, T](this.name, db.newCursor[(String, Long), T])


  @inline def apply(id: Long)(implicit db: DBSessionQueryable): Option[T] = {
    db.get[(String, Long), T]((this.name, id))
  }

  @inline def update(id: Long, value: T)(implicit db: DBSessionUpdatable) = {
    db.put(id, value)
  }

  private class UniqueIndexImpl[K: Packer](name: String, keyColumns: Seq[DBObjectType[T]#Column[_]]) extends UniqueIndex[K, T] {
    rootCollection.indexes.put(name, this) match {
      case Some(old: UniqueIndex[_, T]) =>
        rootCollection.indexes.put(name, old)
        throw new IllegalArgumentException(s"index with name [$name] already exists!")
      case _ =>
    }

    val fullName = rootCollection.name + '.' + name

    def apply(key: K)(implicit db: DBSessionQueryable): Option[T] = {
      db.get[(String, K), T]((this.fullName, key))
    }

    def apply()(implicit db: DBSessionQueryable): Cursor[T] =
      new CursorImpl[K, T](this.fullName, db.newCursor[(String, K), T])

    def apply(start: K, end: K, includeStart: Boolean, includeEnd: Boolean)(implicit db: DBSessionQueryable): Cursor[T] = ???

  }


  def defUniqueIndex[S <: DBObjectType[T], K: Packer](name: String, column: S#Column[K]): UniqueIndex[K, T] = {
    new UniqueIndexImpl[K](name, Seq(column))
  }

  def defUniqueIndex[S <: DBObjectType[T], K1: Packer, K2: Packer](name: String, column1: S#Column[K1], column2: S#Column[K2]): UniqueIndex[(K1, K2), T] = {
    new UniqueIndexImpl[(K1, K2)](name, Seq(column1, column2))
  }

  def defUniqueIndex[S <: DBObjectType[T], K1: Packer, K2: Packer, K3: Packer](name: String, column1: S#Column[K1], column2: S#Column[K2], column3: S#Column[K3]): UniqueIndex[(K1, K2, K3), T] = {
    new UniqueIndexImpl[(K1, K2, K3)](name, Seq(column1, column2, column3))
  }


  //  private class ExtendIndexImpl[S <: DBObjectType[_], K: Packer](name: String, keyColumns: Seq[DBObjectType[S]#Column[K]]) extends ExtendIndex[K, S] {
  //    val extendFrom = keyColumns.head.owner
  //  }

  def defExtendIndex[S <: DBObjectType[_], K: Packer](name: String, column: S#Column[K]): ExtendIndex[K, T] = ???
}