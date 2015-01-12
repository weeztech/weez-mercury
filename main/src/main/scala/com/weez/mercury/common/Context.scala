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

trait Cursor[T] extends Iterator[T] {
  def close(): Unit
}

trait IndexBase[K, V] {
  def apply()(implicit db: DBSessionQueryable): Cursor[V]

  //  def apply(start: K, end: K, excludeStart: Boolean = false, excludeEnd: Boolean = false)(implicit db: DBSessionQueryable): Cursor[V]
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

  def nameInDB: String

  def column[CT](name: String) = Column[CT](name)

  def extend[ST <: DBObjectType[_]](name: String, source: ST) = Extend[ST](name, source)

  val id = column[Long]("id")

  case class Extend[ST](name: String, source: ST)

  case class Column[CT](name: String)

}

trait KeyCollection[T] {
  def apply()(implicit db: DBSessionQueryable): Cursor[T]

  def apply(id: Long)(implicit db: DBSessionQueryable): Option[T]

  def update(id: Long, value: T)(implicit db: DBSessionUpdatable): Unit

  def defUniqueIndex[S <: DBObjectType[T], K: Packer](name: String, column: S#Column[K]): UniqueIndex[K, T]

  def defExtendIndex[S <: DBObjectType[_], K: Packer](name: String, column: S#Column[K]): ExtendIndex[K, T]
}

abstract class RootCollection[T: Packer] extends KeyCollection[T] {
  def name: String = ???

  val indexes = collection.mutable.Map[String, UniqueIndex[_, T]]()


  @inline def apply()(implicit db: DBSessionQueryable): Cursor[T] = {
    val dbCursor = db.newCursor[Long, T]
    new Cursor[T] {
      def next() = dbCursor.next()._2

      def hasNext = dbCursor.hasNext

      def close() = dbCursor.close()
    }
  }

  @inline def update(id: Long, value: T)(implicit db: DBSessionUpdatable) = {
    db.put(id, value)
  }

  @inline def apply(id: Long)(implicit db: DBSessionQueryable): Option[T] = {
    db.get(id)
  }

  private class UniqueIndexImpl[K: Packer](val name: String, keyColumns: Seq[DBObjectType[T]#Column[_]]) extends UniqueIndex[K, T] {
    def apply(key: K)(implicit db: DBSessionQueryable): Option[T] = ???

    def apply()(implicit db: DBSessionQueryable): Cursor[T] = ???
  }

  @inline def putIndex[K](index: UniqueIndexImpl[K]): UniqueIndex[K, T] = {
    this.indexes.put(index.name, index) match {
      case Some(old: UniqueIndex[_, T]) =>
        this.indexes.put(index.name, old)
        throw new IllegalArgumentException(s"index with name [${index.name}] already exists!")
      case _ =>
        index
    }
  }

  def defUniqueIndex[S <: DBObjectType[T], K: Packer](name: String, column: S#Column[K]): UniqueIndex[K, T] = {
    putIndex(new UniqueIndexImpl[K](name, Seq(column)))
  }

  def defUniqueIndex2[S <: DBObjectType[T], K1: Packer, K2: Packer](name: String, column1: S#Column[K1], column2: S#Column[K2]): UniqueIndex[(K1, K2), T] = {
    val index = new UniqueIndexImpl[(K1, K2)](name, Seq(column1, column2))(Packer.tuple[K1, K2])
    putIndex(index)
  }

  def defUniqueIndex3[S <: DBObjectType[T], K1: Packer, K2: Packer, K3: Packer](name: String, column1: S#Column[K1], column2: S#Column[K2], column3: S#Column[K3]): UniqueIndex[(K1, K2, K3), T] = {
    putIndex(new UniqueIndexImpl[(K1, K2, K3)](name, Seq(column1, column2, column3))(Packer.tuple[K1, K2,K3]))
  }

  def defExtendIndex[S <: DBObjectType[_], K: Packer](name: String, column: S#Column[K]): ExtendIndex[K, T] = ???
}