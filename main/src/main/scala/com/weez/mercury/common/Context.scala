package com.weez.mercury.common


import akka.event.LoggingAdapter
import com.weez.mercury.collect
import com.weez.mercury.common._
import com.weez.mercury.common.EntityCollections._

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

trait IndexBase[K, V] {

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
trait Entity extends AnyRef {
  private[common] var _id: Long = 0

  def id: Long = _id

  def newRef() = RefSome[this.type](this.id)
}

trait EntityCollectionListener[-V <: Entity] {
  def canListenEntityInsert: Boolean = true

  def onEntityInsert(newEntity: V)(implicit db: DBSessionUpdatable): Unit

  def canListenEntityUpdate: Boolean = true

  def onEntityUpdate(oldEntity: V, newEntity: V)(implicit db: DBSessionUpdatable): Unit

  def canListenEntityDelete: Boolean = true

  def onEntityDelete(oldEntity: V)(implicit db: DBSessionUpdatable): Unit
}

@collect
trait EntityCollection[V <: Entity] {
  def name: String

  def insert(value: V)(implicit db: DBSessionUpdatable): Long

  def update(value: V)(implicit db: DBSessionUpdatable): Unit

  def delete(value: V)(implicit db: DBSessionUpdatable): Unit

  def defUniqueIndex[K: Packer](name: String, getKey: V => K): UniqueIndex[K, V]

  def defIndex[K: Packer](name: String, getKey: V => K): Index[K, V]

  def defMIndex[K: Packer](name: String, getKeys: V => Seq[K]): Index[K, V]

  def newEntityID()(implicit db: DBSessionUpdatable): Long

  def fxFunc: V => Seq[String] = null

  def apply(word: String)(implicit db: DBSessionQueryable): Cursor[V]

  def scanRefers(id: Long)(implicit db: DBSessionQueryable): Cursor[Entity]
}

object Collections {
  def scanFX(word: String)(implicit db: DBSessionQueryable): Cursor[Entity] = FullTextSearchIndex.scan(word, null.asInstanceOf[HostCollectionImpl[Entity]])

  def scanRefers(id: Long)(implicit db: DBSessionQueryable): Cursor[Entity] = RefReverseIndex.scan(id,null.asInstanceOf[HostCollectionImpl[Entity]])
}


trait SubEntityPair[O <: Entity, V <: Entity] extends Entity {
  val owner: Ref[O]
  val entity: V
}

abstract class SubCollection[O <: Entity, V <: Entity : Packer](ownerRef: Ref[O]) extends EntityCollection[V] {
  def this(owner: O) = this(owner.newRef())

  @inline final override def insert(value: V)(implicit db: DBSessionUpdatable): Long = host.insert(ownerRef, value)

  @inline final override def update(value: V)(implicit db: DBSessionUpdatable): Unit = host.update(ownerRef, value)


  @inline final override def delete(value: V)(implicit db: DBSessionUpdatable): Unit = host.delete(value.id)

  @inline final override def apply(word: String)(implicit db: DBSessionQueryable): Cursor[V] = FullTextSearchIndex.scan(word, host).map(v => v.entity)

  @inline final override def scanRefers(id: Long)(implicit db: DBSessionQueryable): Cursor[V] = RefReverseIndex.scan(id,host).map(e=>e.entity)

  @inline final def defUniqueIndex[K: Packer](name: String, getKey: V => K): UniqueIndex[K, V] = host.defUniqueIndex[K](ownerRef, name, getKey)


  @inline final def defIndex[K: Packer](name: String, getKey: V => K): Index[K, V] = host.defIndex[K](ownerRef, name, getKey)


  @inline final def defMIndex[K: Packer](name: String, getKeys: V => Seq[K]): Index[K, V] = host.defMKIndex[K](ownerRef, name, getKeys)

  @inline final def addListener(listener: EntityCollectionListener[SubEntityPair[O, V]]) = host.addListener(listener)

  @inline final def removeListener(listener: EntityCollectionListener[SubEntityPair[O, V]]) = host.removeListener(listener)


  @inline final override def newEntityID()(implicit db: DBSessionUpdatable): Long = host.newEntityID()


  private[common] lazy val host: SubHostCollectionImpl[O, V] = forSubCollection[O, V](this)
}

abstract class RootCollection[V <: Entity : Packer] extends EntityCollection[V] {

  @inline final override def newEntityID()(implicit db: DBSessionUpdatable): Long = impl.newEntityID()

  @inline final override def insert(value: V)(implicit db: DBSessionUpdatable): Long = impl.insert(value)

  @inline final override def delete(value: V)(implicit db: DBSessionUpdatable): Unit = impl.delete(value.id)

  @inline final override def update(value: V)(implicit db: DBSessionUpdatable): Unit = impl.update(value)

  @inline final def delete(id: Long)(implicit db: DBSessionUpdatable): Unit = impl.delete(id)

  @inline final def apply(id: Long)(implicit db: DBSessionQueryable): Option[V] = impl.get(id)

  @inline final override def defUniqueIndex[K: Packer](name: String, getKey: (V) => K): UniqueIndex[K, V] = impl.defUniqueIndex(name, getKey)

  @inline final override def defMIndex[K: Packer](name: String, getKeys: V => Seq[K]): Index[K, V] = impl.defMKIndex[K](name, getKeys)

  @inline final override def defIndex[K: Packer](name: String, getKey: V => K): Index[K, V] = impl.defIndex[K](name, getKey)

  @inline final def defUniqueKeyView: DataViewBuilder[V, UniqueKeyView] = impl.rootTracer.defUniqueKeyView

  @inline final def defDataView: DataViewBuilder[V, DataView] = impl.rootTracer.defDataView

  @inline final def apply()(implicit db: DBSessionQueryable): Cursor[V] = impl.scan(forward = true)

  @inline final def apply(forward: Boolean)(implicit db: DBSessionQueryable): Cursor[V] = impl.scan(forward)

  @inline final override def apply(word: String)(implicit db: DBSessionQueryable): Cursor[V] = FullTextSearchIndex.scan(word, impl)

  @inline final override def scanRefers(id: Long)(implicit db: DBSessionQueryable): Cursor[V] = RefReverseIndex.scan(id,impl)

  @inline final def addListener(listener: EntityCollectionListener[V]) = impl.addListener(listener)

  @inline final def removeListener(listener: EntityCollectionListener[V]) = impl.removeListener(listener)


  private[common] val impl = newHost[V](name, fxFunc)
}

trait DataView[K, V, E <: Entity] {

  @inline final def apply()(implicit db: DBSessionQueryable): Cursor[(K, V, Ref[E])] = apply(forward = true)

  @inline final def apply(forward: Boolean)(implicit db: DBSessionQueryable): Cursor[(K, V, Ref[E])] = apply(Range.All, forward)

  def apply(range: Range[K], forward: Boolean = true)(implicit db: DBSessionQueryable): Cursor[(K, V, Ref[E])]
}

trait UniqueKeyView[K, V, E <: Entity] extends DataView[K, V, E] {
  def apply(key: K)(implicit db: DBSessionQueryable): Option[V]

  def getRV(key: K)(implicit db: DBSessionQueryable): Option[(Ref[E], V)]
}

import scala.language.higherKinds

trait DataViewBuilder[E <: Entity, DW[_, _, _ <: Entity]] {
  def apply[DK: Packer, DV: Packer, T](name: String, trace: E => T)(extract: T => Map[DK, DV]): DW[DK, DV, E]

  def apply[DK: Packer, DV: Packer, T, R1, R1T](name: String, trace: E => T, r1Path: String, r1Trace: R1 => R1T)(extract: (T, R1T) => Map[DK, DV]): DW[DK, DV, E]

  def apply[DK: Packer, DV: Packer, T, R1, R1T, R2, R2T](name: String, trace: E => T, r1Path: String, r1Trace: R1 => R1T
                                                         , r2Path: String, r2Trace: R2 => R2T)
                                                        (extract: (T, R1T, R2T) => Map[DK, DV]): DW[DK, DV, E]

  def apply[DK: Packer, DV: Packer, T, R1, R1T, R2, R2T, R3, R3T](name: String, trace: E => T, r1Path: String, r1Trace: R1 => R1T,
                                                                  r2Path: String, r2Trace: R2 => R2T,
                                                                  r3Path: String, r3Trace: R3 => R3T)
                                                                 (extract: (T, R1T, R2T, R3T) => Map[DK, DV]): DW[DK, DV, E]

  def apply[DK: Packer, DV: Packer, T, R1, R1T, R2, R2T, R3, R3T, R4, R4T](name: String, trace: E => T, r1Path: String, r1Trace: R1 => R1T,
                                                                           r2Path: String, r2Trace: R2 => R2T,
                                                                           r3Path: String, r3Trace: R3 => R3T,
                                                                           r4Path: String, r4Trace: R4 => R4T)
                                                                          (extract: (T, R1T, R2T, R3T, R4T) => Map[DK, DV]): DW[DK, DV, E]

  def apply[DK: Packer, DV: Packer, T, R1, R1T, R2, R2T, R3, R3T, R4, R4T, R5, R5T](name: String, trace: E => T, r1Path: String, r1Trace: R1 => R1T,
                                                                                    r2Path: String, r2Trace: R2 => R2T,
                                                                                    r3Path: String, r3Trace: R3 => R3T,
                                                                                    r4Path: String, r4Trace: R4 => R4T,
                                                                                    r5Path: String, r5Trace: R5 => R5T)
                                                                                   (extract: (T, R1T, R2T, R3T, R4T, R5T) => Map[DK, DV]): DW[DK, DV, E]
}

case class B(b: Int)

case class A(a: Int, b: B) {
}
