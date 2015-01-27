package com.weez.mercury.common

import akka.event.LoggingAdapter
import com.weez.mercury.collect
import com.weez.mercury.common.EntityCollections.SubHostCollectionImpl

trait Context extends RangeImplicits {
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

  def getRootCollectionMeta(name: String)(implicit db: DBSessionQueryable): DBType.CollectionMeta
}

trait DBSessionUpdatable extends DBSessionQueryable {
  def newEntityId(): Long

  def put[K: Packer, V: Packer](key: K, value: V): Unit

  def del[K: Packer](key: K): Unit
}

trait Cursor[+T <: Entity] extends Iterator[T] with AutoCloseable {
  override def slice(from: Int, until: Int): Cursor[T] = throw new UnsupportedOperationException

  @inline final override def take(n: Int): Cursor[T] = slice(0, n)

  @inline final override def drop(n: Int): Cursor[T] = slice(n, Int.MaxValue)

  @inline final override def toSeq = to[Seq]

  @inline final override def toTraversable: Traversable[T] = toSeq

  @inline final override def toIterable: Iterable[T] = toSeq

  /**
   * DO NOT CALL THIS METHOD.
   * delayed db operation may cause unexpected errors.
   */
  @inline final override def toStream = ???

  override def close(): Unit
}

trait IndexBase[K, V <: Entity] {

  @inline final def apply()(implicit db: DBSessionQueryable): Cursor[V] =
    this.apply(Range.All)

  def apply(range: Range[K], forward: Boolean = true)(implicit db: DBSessionQueryable): Cursor[V]
}

/**
 * RootCollection.Index : (indexID:Int, userKey: K, entityID: Long) -> Long
 * SubCollection.Index : (indexID:Int, ownerID: Long, userKey: K, entityID: Long) -> Long
 * @tparam K Key
 * @tparam V Entity
 */
trait Index[K, V <: Entity] extends IndexBase[K, V]

/**
 * RootCollection.UniqueIndex : (indexID:Int, userKey: K) ->Long
 * SubCollection.UniqueIndex : (indexID:Int, ownerID: Long, userKey: K) -> Long
 * @tparam K Key
 * @tparam V Entity
 */
trait UniqueIndex[K, V <: Entity] extends IndexBase[K, V] {
  self =>
  def apply(key: K)(implicit db: DBSessionQueryable): Option[V]

  def delete(key: K)(implicit db: DBSessionUpdatable): Unit

  def update(value: V)(implicit db: DBSessionUpdatable): Unit

}

trait Ref[+T <: Entity] {
  def id: Long

  @inline final def isEmpty: Boolean = id == 0l

  @inline final def isDefined = id != 0l

  def apply()(implicit db: DBSessionQueryable): T
}

@collect
trait Entity {
  def id: Long

  def newRef() = RefSome[this.type](this.id)
}

import scala.reflect.runtime.universe.TypeTag

trait EntityCollectionListener[V <: Entity] {

  def onEntityInsert(newEntity: V)(implicit db: DBSessionUpdatable): Unit

  def onEntityUpdate(oldEntity: V, newEntity: V)(implicit db: DBSessionUpdatable): Unit

  def onEntityDelete(oldEntity: V)(implicit db: DBSessionUpdatable): Unit

}

@collect
trait EntityCollection[V <: Entity] {
  def name: String

  //add or modify
  def update(value: V)(implicit db: DBSessionUpdatable): Unit

  def delete(value: V)(implicit db: DBSessionUpdatable): Unit

  def defUniqueIndex[K: Packer : TypeTag](name: String, getKey: V => K): UniqueIndex[K, V]

  def defIndex[K: Packer : TypeTag](name: String, getKey: V => K): Index[K, V]

  def addListener(listener: EntityCollectionListener[V]): Unit

  def removeListener(listener: EntityCollectionListener[V]): Unit
}

abstract class SubCollection[V <: Entity : Packer : TypeTag](owner: Entity) extends EntityCollection[V] {
  val ownerID = owner.id

  @inline final override def update(value: V)(implicit db: DBSessionUpdatable): Unit = {
    this.host.update(ownerID, value)
  }

  @inline final override def delete(value: V)(implicit db: DBSessionUpdatable): Unit = {
    this.host.delete(value.id)
  }

  @inline final override def defUniqueIndex[K: Packer : TypeTag](name: String, getKey: V => K): UniqueIndex[K, V] = {
    this.host.defUniqueIndex[K](ownerID, name, getKey)
  }


  @inline final override def defIndex[K: Packer : TypeTag](name: String, getKey: V => K): Index[K, V] = {
    this.host.defIndex[K](ownerID, name, getKey)
  }

  @inline final override def addListener(listener: EntityCollectionListener[V]) = {
    this.host.addSubListener(listener)
  }

  @inline final override def removeListener(listener: EntityCollectionListener[V]) = {
    this.host.removeSubListener(listener)
  }

  private[common] lazy val host: SubHostCollectionImpl[V] = EntityCollections.forPartitionHost[V](this)
}

abstract class RootCollection[V <: Entity : Packer : TypeTag] extends EntityCollection[V] with UniqueIndex[Long, V] {
  private[common] val impl = EntityCollections.newHost[V](name)

  @inline final override def delete(value: V)(implicit db: DBSessionUpdatable): Unit = impl.delete(value.id)

  @inline final override def update(value: V)(implicit db: DBSessionUpdatable): Unit = impl.update(value)

  @inline final override def delete(id: Long)(implicit db: DBSessionUpdatable): Unit = impl.delete(id)

  @inline final override def apply(id: Long)(implicit db: DBSessionQueryable): Option[V] = impl(id)

  @inline final override def defUniqueIndex[K: Packer : TypeTag](name: String, getKey: (V) => K): UniqueIndex[K, V] = impl.defUniqueIndex(name, getKey)

  @inline final override def defIndex[K: Packer : TypeTag](name: String, getKey: V => K): Index[K, V] = impl.defIndex[K](name, getKey)

  @inline final override def apply(range: Range[Long], forward: Boolean = true)(implicit db: DBSessionQueryable): Cursor[V] =
    impl(range, forward)

  @inline final override def addListener(listener: EntityCollectionListener[V]) = {
    impl.addListener(listener)
  }

  @inline final override def removeListener(listener: EntityCollectionListener[V]) = {
    impl.removeListener(listener)
  }
}
