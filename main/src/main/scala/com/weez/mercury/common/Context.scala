package com.weez.mercury.common

import akka.event.LoggingAdapter
import com.weez.mercury.collect
import com.weez.mercury.common.EntityCollections.{DataViewImpl, SubHostCollectionImpl}

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
  private[common] var _id: Long = 0

  def id: Long = _id

  def newRef() = RefSome[this.type](this.id)
}

import scala.reflect.runtime.universe.TypeTag

trait EntityCollectionListener[-V <: Entity] {
  val canListenEntityInsert: Boolean = true

  def onEntityInsert(newEntity: V)(implicit db: DBSessionUpdatable): Unit

  val canListenEntityUpdate: Boolean = true

  def onEntityUpdate(oldEntity: V, newEntity: V)(implicit db: DBSessionUpdatable): Unit

  val canListenEntityDelete: Boolean = true

  def onEntityDelete(oldEntity: V)(implicit db: DBSessionUpdatable): Unit
}


@collect
trait EntityCollection[V <: Entity] {
  def name: String

  def insert(value: V)(implicit db: DBSessionUpdatable): Long

  def update(value: V)(implicit db: DBSessionUpdatable): Unit

  def delete(value: V)(implicit db: DBSessionUpdatable): Unit

  def defUniqueIndex[K: Packer : TypeTag](name: String, getKey: V => K): UniqueIndex[K, V]

  def defIndex[K: Packer : TypeTag](name: String, getKey: V => K): Index[K, V]

  def newEntityID()(implicit db: DBSessionUpdatable): Long
}

trait SubEntityPair[O <: Entity, V <: Entity] extends Entity {
  val owner: Ref[O]
  val entity: V
}

abstract class SubCollection[O <: Entity, V <: Entity : Packer : TypeTag](ownerRef: Ref[O]) extends EntityCollection[V] {
  def this(owner: O) = this(owner.newRef())

  @inline final override def insert(value: V)(implicit db: DBSessionUpdatable): Long = host.insert(ownerRef, value)

  @inline final override def update(value: V)(implicit db: DBSessionUpdatable): Unit = host.update(ownerRef, value)


  @inline final override def delete(value: V)(implicit db: DBSessionUpdatable): Unit = host.delete(value.id)


  @inline final override def defUniqueIndex[K: Packer : TypeTag](name: String, getKey: V => K): UniqueIndex[K, V] = host.defUniqueIndex[K](ownerRef, name, getKey)


  @inline final override def defIndex[K: Packer : TypeTag](name: String, getKey: V => K): Index[K, V] = host.defIndex[K](ownerRef, name, getKey)


  @inline final def addListener(listener: EntityCollectionListener[SubEntityPair[O, V]]) = host.addListener(listener)


  @inline final def removeListener(listener: EntityCollectionListener[SubEntityPair[O, V]]) = host.removeListener(listener)


  @inline final override def newEntityID()(implicit db: DBSessionUpdatable): Long = host.newEntityID()

  private[common] lazy val host: SubHostCollectionImpl[O, V] = EntityCollections.forPartitionHost[O, V](this)
}

abstract class RootCollection[V <: Entity : Packer : TypeTag] extends EntityCollection[V] {

  @inline final override def newEntityID()(implicit db: DBSessionUpdatable): Long = impl.newEntityID()

  @inline final override def insert(value: V)(implicit db: DBSessionUpdatable): Long = impl.insert(value)

  @inline final override def delete(value: V)(implicit db: DBSessionUpdatable): Unit = impl.delete(value.id)

  @inline final override def update(value: V)(implicit db: DBSessionUpdatable): Unit = impl.update(value)

  @inline final def delete(id: Long)(implicit db: DBSessionUpdatable): Unit = impl.delete(id)

  @inline final def apply(id: Long)(implicit db: DBSessionQueryable): Option[V] = impl(id)

  @inline final override def defUniqueIndex[K: Packer : TypeTag](name: String, getKey: (V) => K): UniqueIndex[K, V] = impl.defUniqueIndex(name, getKey)

  @inline final override def defIndex[K: Packer : TypeTag](name: String, getKey: V => K): Index[K, V] = impl.defIndex[K](name, getKey)

  @inline final def apply(forward: Boolean)(implicit db: DBSessionQueryable): Cursor[V] = impl(forward)

  @inline final def apply()(implicit db: DBSessionQueryable): Cursor[V] = impl(forward = true)

  @inline final def addListener(listener: EntityCollectionListener[V]) = impl.addListener(listener)

  @inline final def removeListener(listener: EntityCollectionListener[V]) = impl.removeListener(listener)

  private[common] val impl = EntityCollections.newHost[V](name)
}

abstract class DataView[K: Packer, V: Packer] {
  dataView =>

  def name: String

  @inline final def apply(key: K)(implicit db: DBSessionQueryable): Option[V] = impl(key)

  @inline final def apply(range: Range[K], forward: Boolean = true)(implicit db: DBSessionQueryable): Cursor[V] = impl(range, forward)

  sealed abstract class Tracer[ES <: AnyRef, E <: Entity] {
    tracer =>
    def createEntities(): ES

    def put(entity: E, entities: ES)

    def isChanged(oldEntity: E, newEntity: E): Boolean

    abstract class SubTracer[S <: Entity] extends Tracer[ES, S] {

      def getSubRef(source: E): Ref[S]

      def target: RootCollection[S]

      def refIndex: IndexBase[_ >: Ref[S]
        with Product1[Ref[S]]
        with Product2[Ref[S], Nothing]
        with Product3[Ref[S], Nothing, Nothing]
        with Product4[Ref[S], Nothing, Nothing, Nothing]
        with Product5[Ref[S], Nothing, Nothing, Nothing, Nothing]
        with Product6[Ref[S], Nothing, Nothing, Nothing, Nothing, Nothing]
        with Product7[Ref[S], Nothing, Nothing, Nothing, Nothing, Nothing, Nothing]
        , E]

      final def createEntities() = tracer.createEntities()

      private[common] val impl = tracer.impl.newSub[S](this)
    }

    private[common] val impl: EntityCollections.DataViewImpl[K, V]#Tracer[ES, E]
  }

  abstract class Meta[ES <: AnyRef, ROOT <: Entity] extends Tracer[ES, ROOT] {
    def root: RootCollection[ROOT]

    def extract(entities: ES): Map[K, V]

    private[common] val impl = dataView.impl.newMeta(this)
  }

  private[common] val impl: DataViewImpl[K, V] = EntityCollections.newDataView[K, V](name)
}
case class Test[A,B](a: A,b: B){

  def foreach[U](f: (A,B) => U): Unit = {
    f(a,b)
  }
}

object Test2{
  val t = Test(1,"String")
}