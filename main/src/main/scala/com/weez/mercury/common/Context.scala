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

trait IndexBase[K, V <: Entity] {
  def apply()(implicit db: DBSessionQueryable): Cursor[V]

  def apply(start: K, end: K, includeStart: Boolean = true, includeEnd: Boolean = false)(implicit db: DBSessionQueryable): Cursor[V]
}

trait Index[K, V <: Entity] extends IndexBase[K, V]

trait UniqueIndex[K, V <: Entity] extends IndexBase[K, V] {
  def apply(key: K)(implicit db: DBSessionQueryable): Option[V]
}

trait ExtendIndex[K, B <: Entity, V <: ExtendEntity[B]] extends UniqueIndex[K, V]

trait Ref[+T <: Entity] {
  def isEmpty: Boolean

  def apply()(implicit db: DBSessionQueryable): T
}

trait DBObjectType[T <: Entity] {
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

trait KeyCollection[T <: Entity] {

  def apply()(implicit db: DBSessionQueryable): Cursor[T]

  def apply(id: Long)(implicit db: DBSessionQueryable): Option[T]

  def update(id: Long, value: T)(implicit db: DBSessionUpdatable): Unit

  def defUniqueIndex[K: Packer](name: String, keyGetter: T => K): UniqueIndex[K, T]
}

trait Entity {
  val id: Long
}

trait ExtendEntity[B <: Entity] extends Entity {
  //def base: Ref[B]
}

trait ExtendCollection[T <: ExtendEntity[B], B <: Entity] extends KeyCollection[T] {
  def defExtendIndex[K: Packer](name: String, keyGetter: B => K): ExtendIndex[K, B, T]
}


class CursorImpl[K: Packer, T](prefix: String, dbCursor: DBCursor[(String, K), T]) extends Cursor[T] {

  def next(): T = ???

  def hasNext = dbCursor.hasNext

  def close() = dbCursor.close()
}


abstract class RootCollection[T <: Entity : Packer] extends KeyCollection[T] {
  rootCollection =>

  def name: String = ???

  val indexes = collection.mutable.Map[String, Any]()


  @inline def apply()(implicit db: DBSessionQueryable): Cursor[T] =
    new CursorImpl[Long, T](this.name, db.newCursor[(String, Long), T])


  @inline def apply(id: Long)(implicit db: DBSessionQueryable): Option[T] = {
    db.get[(String, Long), T]((this.name, id))
  }

  @inline def update(id: Long, value: T)(implicit db: DBSessionUpdatable) = {
    db.put(id, value)
  }

  abstract class IndexBaseImpl[K: Packer, KB](name: String, keyGetter: KB => K) extends IndexBase[K, T] {
    rootCollection.indexes.put(name, this) match {
      case Some(old: UniqueIndex[_, T]) =>
        rootCollection.indexes.put(name, old)
        throw new IllegalArgumentException(s"index with name [$name] already exists!")
      case _ =>
    }
    val keyPrefix = rootCollection.name + '.' + name

    def apply(key: K)(implicit db: DBSessionQueryable): Option[T] = {
      db.get[(String, K), T]((keyPrefix, key))
    }

    def apply()(implicit db: DBSessionQueryable): Cursor[T] =
      new CursorImpl[K, T](keyPrefix, db.newCursor[(String, K), T])
  }

  private class UniqueIndexImpl[K: Packer](name: String, keyGetter: T => K) extends IndexBaseImpl[K, T](name, keyGetter) with UniqueIndex[K, T] {
    def apply(start: K, end: K, includeStart: Boolean, includeEnd: Boolean)(implicit db: DBSessionQueryable): Cursor[T] = ???
  }

  def defUniqueIndex[K: Packer](name: String, keyGetter: T => K): UniqueIndex[K, T] = new UniqueIndexImpl[K](name, keyGetter)


}

abstract class ExtendRootCollection[T <: ExtendEntity[B] : Packer, B <: Entity : Packer] extends RootCollection[T] with ExtendCollection[T, B] {

  private class ExtendIndexImpl[K: Packer](name: String, keyGetter: B => K) extends IndexBaseImpl[K, B](name, keyGetter) with ExtendIndex[K, B, T] {
    def apply(start: K, end: K, includeStart: Boolean, includeEnd: Boolean)(implicit db: DBSessionQueryable): Cursor[T] = ???
  }

  def defExtendIndex[K: Packer](name: String, keyGetter: B => K): ExtendIndex[K, B, T] = new ExtendIndexImpl(name, keyGetter)
}
