package com.weez.mercury.common

import akka.event.LoggingAdapter
import com.weez.mercury.common.EntityCollections.HostCollectionImpl

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

trait IndexBase[K, V <: Entity] {
  @inline final def apply()(implicit db: DBSessionQueryable): Cursor[V] =
    this.apply(None, None)

  @inline final def apply(start: K, end: K)(implicit db: DBSessionQueryable): Cursor[V] =
    this.apply(Some(start), Some(end))

  def apply(start: Option[K], end: Option[K], excludeStart: Boolean = false, excludeEnd: Boolean = false, forward: Boolean = true)(implicit db: DBSessionQueryable): Cursor[V]

  def subIndex[PK: Packer, SK](prefix: PK, fullKeyGetter: (PK, SK) => K, subKeyGetter: K => SK): IndexBase[SK, V]
}

trait Index[K, V <: Entity] extends IndexBase[K, V]

trait UniqueIndex[K, V <: Entity] extends IndexBase[K, V] {
  self =>
  def apply(key: K)(implicit db: DBSessionQueryable): Option[V]

  def delete(key: K)(implicit db: DBSessionUpdatable): Unit

  def update(value: V)(implicit db: DBSessionUpdatable): Unit

  override def subIndex[PK: Packer, SK](prefix: PK, fullKey: (PK, SK) => K, subKey: K => SK): UniqueIndex[SK, V] =
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

abstract class PartitionCollection[P: Packer, V <: Entity : Packer] extends EntityCollection[V] {
  val partition: P

  def v2p(value: V): P

  @inline private def checkHost(value: V): Unit = {
    if (!(partition match {
      case id: Long => id == v2p(value).asInstanceOf[Long]
      case els => v2p(value).equals(els)
    })) {
      throw new IllegalArgumentException("value is not in this partition")
    }
  }

  @inline final override def update(value: V)(implicit db: DBSessionUpdatable): Unit = {
    this.checkHost(value)
    this.all.update(value)
  }

  @inline final override def delete(value: V)(implicit db: DBSessionUpdatable): Unit = {
    this.checkHost(value)
    this.all.delete(value.id)
  }

  @inline final def defUniqueIndex[K: Packer](name: String, getKey: V => K): UniqueIndex[K, V] = {
    this.all.asInstanceOf[HostCollectionImpl[V]].defPartitionIndex[P, K](this.partition, name, getKey)
  }

  lazy val all: HostCollection[V] = EntityCollections.forPartitionHost[V](this)
}

trait HostCollection[V <: Entity] extends EntityCollection[V] with UniqueIndex[Long, V] {
  @inline final override def delete(value: V)(implicit db: DBSessionUpdatable): Unit = this.delete(value.id)
}

abstract class RootCollection[V <: Entity : Packer] extends HostCollection[V] {
  private val impl = EntityCollections.newHost[V](name)

  @inline final override def update(value: V)(implicit db: DBSessionUpdatable): Unit = impl.update(value)

  @inline final override def delete(id: Long)(implicit db: DBSessionUpdatable): Unit = impl.delete(id)

  @inline final override def apply(id: Long)(implicit db: DBSessionQueryable): Option[V] = impl(id)

  @inline final override def defUniqueIndex[K: Packer](name: String, getKey: (V) => K): UniqueIndex[K, V] = impl.defUniqueIndex(name, getKey)

  @inline final override def apply(start: Option[Long], end: Option[Long], excludeStart: Boolean, excludeEnd: Boolean, forward: Boolean)(implicit db: DBSessionQueryable): Cursor[V] =
    impl(start, end, excludeStart, excludeEnd, forward)
}

trait KeyCollection[T <: Entity] {

  def update(value: T)(implicit db: DBSessionUpdatable): Unit

  def defUniqueIndex[K: Packer](name: String, keyGetter: T => K): UniqueIndex[K, T]
}
