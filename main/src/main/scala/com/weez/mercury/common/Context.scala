package com.weez.mercury.common

import akka.actor.Actor
import akka.event.LoggingAdapter
import EntityCollections._

trait Context extends RangeImplicits {
  implicit val context: this.type = this

  def log: LoggingAdapter

  def request: ModelObject

  def response: ModelObject

  def complete(response: ModelObject): Unit

  def peer: String

  def acceptUpload(receiver: => Actor): String

  private[common] def app: Application
}

trait UploadContext {
  def id: String

  def peer: String

  def request: ModelObject

  def sessionState: Option[SessionState]

  def finish(response: ModelObject): Unit

  def finishWith[C](f: C => Unit)(implicit evidence: ContextType[C]): Unit

  def fail(ex: Throwable): Unit
}

case object UploadResume

case class UploadData(buf: akka.util.ByteString, context: UploadContext)

case class UploadEnd(context: UploadContext)

sealed trait ContextType[C] {
  def withSessionState: Boolean

  def withDBQueryable: Boolean

  def withDBUpdatable: Boolean
}

object ContextType {

  class Evidence[C] private[ContextType](val withSessionState: Boolean, val withDBQueryable: Boolean, val withDBUpdatable: Boolean) extends ContextType[C]

  implicit val pure = new Evidence[Context](withSessionState = false, withDBQueryable = false, withDBUpdatable = false)
  implicit val withSession = new Evidence[Context with SessionState](withSessionState = true, withDBQueryable = false, withDBUpdatable = false)
  implicit val withQuery = new Evidence[Context with DBSessionQueryable](withSessionState = false, withDBQueryable = true, withDBUpdatable = false)
  implicit val withSessionAndQuery = new Evidence[Context with SessionState with DBSessionQueryable](withSessionState = true, withDBQueryable = true, withDBUpdatable = false)
  implicit val withSessionAndUpdate = new Evidence[Context with SessionState with DBSessionUpdatable](withSessionState = true, withDBQueryable = true, withDBUpdatable = true)
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

  def apply()(implicit db: DBSessionQueryable): Cursor[V]

  def apply[P: Packer](range: Range[P], forward: Boolean = true)(implicit db: DBSessionQueryable, canUse: TuplePrefixed[K, P]): Cursor[V]
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

  @inline final def id: Long = _id

  @inline final def id_=(id: Long) = _id = id

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

  def fxFunc: Option[(V, DBSessionQueryable) => Seq[String]] = None

  def apply(word: String)(implicit db: DBSessionQueryable): Cursor[V]

  def scanRefers(id: Long)(implicit db: DBSessionQueryable): Cursor[Entity]
}

object Collections {

  def entity(id: Long)(implicit db: DBSessionQueryable): Option[Entity] = getEntityO[Entity](id)

  def scanFX(word: String)(implicit db: DBSessionQueryable): Cursor[Entity] = FullTextSearchIndex.scan(word, null.asInstanceOf[HostCollectionImpl[Entity]])

  def scanRefers(id: Long)(implicit db: DBSessionQueryable): Cursor[Entity] = RefReverseIndex.scan(id, null.asInstanceOf[HostCollectionImpl[Entity]])
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

  @inline final override def scanRefers(id: Long)(implicit db: DBSessionQueryable): Cursor[V] = RefReverseIndex.scan(id, host).map(e => e.entity)

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

  @inline final def apply()(implicit db: DBSessionQueryable): Cursor[V] = impl.scan(forward = true)

  @inline final def apply(forward: Boolean)(implicit db: DBSessionQueryable): Cursor[V] = impl.scan(forward)

  @inline final override def apply(word: String)(implicit db: DBSessionQueryable): Cursor[V] = FullTextSearchIndex.scan(word, impl)

  @inline final override def scanRefers(id: Long)(implicit db: DBSessionQueryable): Cursor[V] = RefReverseIndex.scan(id, impl)

  @inline final def addListener(listener: EntityCollectionListener[V]) = impl.addListener(listener)

  @inline final def removeListener(listener: EntityCollectionListener[V]) = impl.removeListener(listener)

  private[common] val impl = newHost[V](name, fxFunc)
}

final case class KeyValue[K, V](key: K, value: V)

trait Merger[V] {
  def add(v1: V, v2: V): Option[V]

  def sub(v1: V, v2: V): Option[V]
}

abstract class DataView[K: Packer, V: Packer] {
  def name: String

  @inline final def apply()(implicit db: DBSessionQueryable): Cursor[KeyValue[K, V]] = apply(forward = true)

  @inline final def apply(forward: Boolean)(implicit db: DBSessionQueryable): Cursor[KeyValue[K, V]] = impl(forward)


  @inline final def apply[P: Packer](range: Range[P], forward: Boolean = true)(implicit db: DBSessionQueryable, canUse: TuplePrefixed[K, P]): Cursor[KeyValue[K, V]] = impl(range, forward)

  @inline final def defExtractor[E <: Entity](collection: EntityCollection[E])(f: (E, DBSessionQueryable) => scala.collection.Map[K, V]) = impl.defExtractor(collection, f)

  @inline final def defExtractor[DK, DV](dataView: DataView[DK, DV])(f: (DK, DV, DBSessionQueryable) => scala.collection.Map[K, V]) = impl.defExtractor(dataView.impl, f)

  protected def defMerger: Option[Merger[V]] = None

  private val impl = EntityCollections.newDataView[K, V](name, defMerger.orNull)
}