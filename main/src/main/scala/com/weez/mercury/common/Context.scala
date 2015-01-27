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

  def newEntityID()(implicit db: DBSessionUpdatable): Long
}

abstract class SubCollection[V <: Entity : Packer : TypeTag](owner: Entity) extends EntityCollection[V] {

  @inline final override def update(value: V)(implicit db: DBSessionUpdatable): Unit = host.update(ownerID, value)


  @inline final override def delete(value: V)(implicit db: DBSessionUpdatable): Unit = host.delete(value.id)


  @inline final override def defUniqueIndex[K: Packer : TypeTag](name: String, getKey: V => K): UniqueIndex[K, V] = host.defUniqueIndex[K](ownerID, name, getKey)


  @inline final override def defIndex[K: Packer : TypeTag](name: String, getKey: V => K): Index[K, V] = host.defIndex[K](ownerID, name, getKey)


  @inline final override def addListener(listener: EntityCollectionListener[V]) = host.addSubListener(listener)


  @inline final override def removeListener(listener: EntityCollectionListener[V]) = host.removeSubListener(listener)


  @inline final override def newEntityID()(implicit db: DBSessionUpdatable): Long = host.newEntityID()

  val ownerID = owner.id

  private[common] lazy val host: SubHostCollectionImpl[V] = EntityCollections.forPartitionHost[V](this)
}

abstract class RootCollection[V <: Entity : Packer : TypeTag] extends EntityCollection[V] {
  private[common] val impl = EntityCollections.newHost[V](name)

  @inline final override def newEntityID()(implicit db: DBSessionUpdatable): Long = impl.newEntityID()

  @inline final override def delete(value: V)(implicit db: DBSessionUpdatable): Unit = impl.delete(value.id)

  @inline final override def update(value: V)(implicit db: DBSessionUpdatable): Unit = impl.update(value)

  @inline final def delete(id: Long)(implicit db: DBSessionUpdatable): Unit = impl.delete(id)

  @inline final def apply(id: Long)(implicit db: DBSessionQueryable): Option[V] = impl(id)

  @inline final override def defUniqueIndex[K: Packer : TypeTag](name: String, getKey: (V) => K): UniqueIndex[K, V] = impl.defUniqueIndex(name, getKey)

  @inline final override def defIndex[K: Packer : TypeTag](name: String, getKey: V => K): Index[K, V] = impl.defIndex[K](name, getKey)

  @inline final def apply()(implicit db: DBSessionQueryable): Cursor[V] = impl()

  @inline final override def addListener(listener: EntityCollectionListener[V]) = impl.addListener(listener)

  @inline final override def removeListener(listener: EntityCollectionListener[V]) = impl.removeListener(listener)

}
