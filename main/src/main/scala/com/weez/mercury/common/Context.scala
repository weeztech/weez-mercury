package com.weez.mercury.common

import akka.event.LoggingAdapter
import com.weez.mercury.common.EntityCollections.SubHostCollectionImpl

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

  def exists[K: Packer](key: K): Boolean

  def newCursor(): DBCursor

  private[common] def schema: DBSchema
}

trait DBSessionUpdatable extends DBSessionQueryable {
  def put[K: Packer, V: Packer](key: K, value: V): Unit

  def del[K: Packer](key: K): Unit
}

trait Cursor[+T <: Entity] extends Iterator[T] with AutoCloseable {
  override def slice(from: Int, until: Int): Cursor[T] = throw new UnsupportedOperationException

  @inline final override def take(n: Int): Cursor[T] = slice(0, n)

  @inline final override def drop(n: Int): Cursor[T] = slice(n, Int.MaxValue)

  override def close(): Unit
}

trait SubKeyMapper[K, PK, SK] {
  def prefixKey: PK

  def fullKey(subKey: SK): K

  def subKey(fullKey: K): SK
}

trait IndexBase[K, V <: Entity] {
  @inline final def apply()(implicit db: DBSessionQueryable): Cursor[V] =
    this.apply(None, None)

  @inline final def apply(start: K, end: K)(implicit db: DBSessionQueryable): Cursor[V] =
    this.apply(Some(start), Some(end))

  def apply(start: Option[K], end: Option[K], excludeStart: Boolean = false, excludeEnd: Boolean = false, forward: Boolean = true)(implicit db: DBSessionQueryable): Cursor[V]

  def subIndex[PK: Packer, SK](keyMapper: SubKeyMapper[K, PK, SK]): IndexBase[SK, V]
}

trait Index[K, V <: Entity] extends IndexBase[K, V]

trait UniqueIndex[K, V <: Entity] extends IndexBase[K, V] {
  self =>
  def apply(key: K)(implicit db: DBSessionQueryable): Option[V]

  def delete(key: K)(implicit db: DBSessionUpdatable): Unit

  def update(value: V)(implicit db: DBSessionUpdatable): Unit

  override def subIndex[PK: Packer, SK](keyMapper: SubKeyMapper[K, PK, SK]): UniqueIndex[SK, V] =
    throw new UnsupportedOperationException()
}

trait Ref[+T <: Entity] {
  def id: Long

  @inline final def isEmpty: Boolean = id == 0l

  @inline final def isDefined = id != 0l

  def apply()(implicit db: DBSessionQueryable): T
}

trait Entity {
  def id: Long

  def newRef() = RefSome[this.type](this.id)
}

trait EntityCollection[V <: Entity] {
  val name: String

  //add or modify
  def update(value: V)(implicit db: DBSessionUpdatable): Unit

  def delete(value: V)(implicit db: DBSessionUpdatable): Unit

  def defUniqueIndex[K: Packer](name: String, getKey: V => K): UniqueIndex[K, V]
}

abstract class SubCollection[V <: Entity : Packer](owner: Entity) extends EntityCollection[V] {
  val ownerID = owner.id

  @inline final override def update(value: V)(implicit db: DBSessionUpdatable): Unit = {
    this.host.update(ownerID, value)
  }

  @inline final override def delete(value: V)(implicit db: DBSessionUpdatable): Unit = {
    this.host.delete(value.id)
  }

  @inline final override def defUniqueIndex[K: Packer](name: String, getKey: V => K): UniqueIndex[K, V] = {
    this.host.defUniqueIndex[K](ownerID, name, getKey)
  }

  private lazy val host: SubHostCollectionImpl[V] = EntityCollections.forPartitionHost[V](this)
}

abstract class RootCollection[V <: Entity : Packer] extends EntityCollection[V] with UniqueIndex[Long, V] {
  private val impl = EntityCollections.newHost[V](name)

  @inline final override def delete(value: V)(implicit db: DBSessionUpdatable): Unit = impl.delete(value.id)

  @inline final override def update(value: V)(implicit db: DBSessionUpdatable): Unit = impl.update(value)

  @inline final override def delete(id: Long)(implicit db: DBSessionUpdatable): Unit = impl.delete(id)

  @inline final override def apply(id: Long)(implicit db: DBSessionQueryable): Option[V] = impl(id)

  @inline final override def defUniqueIndex[K: Packer](name: String, getKey: (V) => K): UniqueIndex[K, V] = impl.defUniqueIndex(name, getKey)

  @inline final override def apply(start: Option[Long], end: Option[Long], excludeStart: Boolean, excludeEnd: Boolean, forward: Boolean)(implicit db: DBSessionQueryable): Cursor[V] =
    impl(start, end, excludeStart, excludeEnd, forward)
}

trait KeyCollection[T <: Entity] {
}
