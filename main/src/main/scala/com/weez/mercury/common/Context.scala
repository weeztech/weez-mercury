package com.weez.mercury.common

import scala.concurrent._
import akka.actor.Actor
import akka.event.LoggingAdapter
import EntityCollections._

trait Context {
  implicit val executor: ExecutionContext

  def response: Response

  def complete(response: Response): Unit

  def acceptUpload(receiver: UploadContext => Unit): String

  def futureQuery[T](f: DBSessionQueryable => T): Future[T]

  def futureUpdate[T](f: DBSessionUpdatable => T): Future[T]

  def app: Application

  def tempDir: FileIO.Path
}

trait RemoteCallContext extends Context with RangeImplicits {
  implicit val context: this.type = this

  def api: String

  def log: LoggingAdapter

  def request: ModelObject

  def peer: String
}

sealed trait Response

sealed trait InstantResponse extends Response

case class FileResponse(path: String) extends InstantResponse

case class ResourceResponse(url: String) extends InstantResponse

case class ModelResponse(obj: ModelObject) extends InstantResponse

case class StreamResponse(actor: () => Actor) extends InstantResponse

case class FutureResponse(x: Future[Response]) extends Response

case class FailureResponse(ex: Throwable) extends InstantResponse


trait UploadContext extends Context {

  import akka.util.ByteString

  def id: String

  def queue: AsyncDequeue[ByteString]

  def sessionState: Option[SessionState]
}

case object UploadResume

case object UploadCancelled

case class UploadData(buf: akka.util.ByteString)

case object UploadEnd

sealed trait UploadResult

case class UploadSuccess(response: InstantResponse) extends UploadResult

case class UploadFailure(ex: Throwable) extends UploadResult

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

  def getMeta(name: String)(implicit db: DBSessionQueryable): DBType.Meta
}

trait DBSessionUpdatable extends DBSessionQueryable {
  def newEntityId(): Long

  def put[K: Packer, V: Packer](key: K, value: V): Unit

  def del[K: Packer](key: K): Unit
}

trait IndexBase[K, V] {

  import com.weez.mercury.macros.TuplePrefixed

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

  def contains(key: K)(implicit db: DBSessionQueryable): Boolean

  def delete(key: K)(implicit db: DBSessionUpdatable): Unit

  def update(value: V)(implicit db: DBSessionUpdatable): Unit
}

trait Ref[+T <: Entity] extends Equals {
  def id: Long

  @inline final def isEmpty: Boolean = id == 0l

  @inline final def isDefined = id != 0l


  def canEqual(that: Any) = that.isInstanceOf[Ref[_]]

  override def equals(obj: Any): Boolean = {
    obj match {
      case x: Ref[_] => x.id == this.id
      case _ => false
    }
  }

  def apply()(implicit db: DBSessionQueryable): T
}

object Ref {
  def apply[T <: Entity](id: Long): Ref[T] = {
    if (id == 0l) {
      RefEmpty
    } else {
      RefSome[T](id)
    }
  }
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

  def insert(value: V)(implicit db: DBSessionUpdatable): Long

  def update(value: V)(implicit db: DBSessionUpdatable): Unit

  def delete(value: V)(implicit db: DBSessionUpdatable): Unit

  def newEntityID()(implicit db: DBSessionUpdatable): Long

  def apply(word: String)(implicit db: DBSessionQueryable): Cursor[V]

  def scanRefers(id: Long)(implicit db: DBSessionQueryable): Cursor[Entity]

  protected def defUniqueIndex[K: Packer](name: String, getKey: V => K): UniqueIndex[K, V]

  protected def defUniqueIndexEx[K: Packer](name: String)(getKey: (V, DBSessionQueryable) => Set[K]): UniqueIndex[K, V]

  protected def defIndex[K: Packer](name: String, getKey: V => K): Index[K, V]

  protected def defIndexEx[K: Packer](name: String)(getKeys: (V, DBSessionQueryable) => Set[K]): Index[K, V]

  @inline protected[common] final def extractTo[D](dataBoard: DataBoard[D])(f: (V, DBSessionQueryable) => Seq[D]): Unit = newEntity2DataBoardExtractor(this, dataBoard.impl, f)

  @inline protected[common] final def extractTo[DK, DV](dataView: DataView[DK, DV])(f: (V, DBSessionQueryable) => Map[DK, DV]): Unit = newEntity2DataViewExtractor(this, dataView.impl, f)
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

  @inline final def insert(value: V)(implicit db: DBSessionUpdatable): Long = host.insert(ownerRef, value)

  @inline final def update(value: V)(implicit db: DBSessionUpdatable): Unit = host.update(ownerRef, value)

  @inline final def delete(value: V)(implicit db: DBSessionUpdatable): Unit = host.delete(value.id)

  @inline final def apply(word: String)(implicit db: DBSessionQueryable): Cursor[V] = FullTextSearchIndex.scan(word, host).map(v => v.entity)

  @inline final def scanRefers(id: Long)(implicit db: DBSessionQueryable): Cursor[V] = RefReverseIndex.scan(id, host).map(e => e.entity)

  @inline final def defUniqueIndex[K: Packer](name: String, getKey: V => K): UniqueIndex[K, V] = host.defUniqueIndex[K](ownerRef, name, getKey)

  @inline final def defUniqueIndexEx[K: Packer](name: String)(getKey: (V, DBSessionQueryable) => Set[K]): UniqueIndex[K, V] = host.defUniqueIndexEx[K](ownerRef, name, getKey)

  @inline final def defIndex[K: Packer](name: String, getKey: V => K): Index[K, V] = host.defIndex[K](ownerRef, name, getKey)

  @inline final def defIndexEx[K: Packer](name: String)(getKeys: (V, DBSessionQueryable) => Set[K]): Index[K, V] = host.defIndexEx[K](ownerRef, name, getKeys)

  @inline final def addListener(listener: EntityCollectionListener[SubEntityPair[O, V]]) = host.addListener(listener)

  @inline final def removeListener(listener: EntityCollectionListener[SubEntityPair[O, V]]) = host.removeListener(listener)

  @inline final override def newEntityID()(implicit db: DBSessionUpdatable): Long = host.newEntityID()

  private[common] lazy val host = forSubCollection[O, V](this)
}

abstract class RootCollection[V <: Entity : Packer] extends EntityCollection[V] {

  @inline final def newEntityID()(implicit db: DBSessionUpdatable): Long = impl.newEntityID()

  @inline final def insert(value: V)(implicit db: DBSessionUpdatable): Long = impl.insert(value)

  @inline final def delete(value: V)(implicit db: DBSessionUpdatable): Unit = impl.delete(value.id)

  @inline final def update(value: V)(implicit db: DBSessionUpdatable): Unit = impl.update(value)

  @inline final def delete(id: Long)(implicit db: DBSessionUpdatable): Unit = impl.delete(id)

  @inline final def apply(id: Long)(implicit db: DBSessionQueryable): Option[V] = impl.get(id)

  @inline final def contains(id: Long)(implicit db: DBSessionQueryable): Boolean = impl.contains(id)

  @inline final def apply()(implicit db: DBSessionQueryable): Cursor[V] = impl.scan(forward = true)

  @inline final def apply(forward: Boolean)(implicit db: DBSessionQueryable): Cursor[V] = impl.scan(forward)

  @inline final def apply(word: String)(implicit db: DBSessionQueryable): Cursor[V] = FullTextSearchIndex.scan(word, impl)

  @inline final def scanRefers(id: Long)(implicit db: DBSessionQueryable): Cursor[V] = RefReverseIndex.scan(id, impl)

  @inline final def addListener(listener: EntityCollectionListener[V]) = impl.addListener(listener)

  @inline final def removeListener(listener: EntityCollectionListener[V]) = impl.removeListener(listener)

  @inline protected final def defUniqueIndex[K: Packer](name: String, getKey: V => K): UniqueIndex[K, V] = impl.defUniqueIndex(name, getKey)

  @inline protected final def defUniqueIndexEx[K: Packer](name: String)(getKey: (V, DBSessionQueryable) => Set[K]): UniqueIndex[K, V] = impl.defUniqueIndexEx(name, getKey)

  @inline protected final def defIndexEx[K: Packer](name: String)(getKeys: (V, DBSessionQueryable) => Set[K]): Index[K, V] = impl.defIndexEx[K](name, getKeys)

  @inline protected final def defIndex[K: Packer](name: String, getKey: V => K): Index[K, V] = impl.defIndex[K](name, getKey)

  private[common] val impl = newHost[V](this)
}

final case class KeyValue[K, V](key: K, value: V)

trait Merger[V] {
  def isEmpty(v1: V): Boolean

  def add(v1: V, v2: V): V

  def neg(v1: V): V
}

trait CanMerge[Repr <: CanMerge[Repr]] extends Equals {
  def isEmpty: Boolean

  def +(x: Repr): Repr

  def -(x: Repr): Repr

  def neg(): Repr
}

import scala.reflect.runtime.universe._

@collect
abstract class DataView[K: Packer, V: Packer : TypeTag] {

  import com.weez.mercury.macros.TuplePrefixed

  @inline final def apply()(implicit db: DBSessionQueryable): Cursor[KeyValue[K, V]] = apply(forward = true)

  @inline final def apply(forward: Boolean)(implicit db: DBSessionQueryable): Cursor[KeyValue[K, V]] = impl(forward)

  @inline final def apply(key: K)(implicit db: DBSessionQueryable): Option[KeyValue[K, V]] = impl(key)

  @inline final def contains(key: K)(implicit db: DBSessionQueryable): Boolean = impl.contains(key)

  @inline final def apply[P: Packer](range: Range[P], forward: Boolean = true)(implicit db: DBSessionQueryable, canUse: TuplePrefixed[K, P]): Cursor[KeyValue[K, V]] = impl(range, forward)

  @inline protected final def extractFrom[E <: Entity](collection: EntityCollection[E])(f: (E, DBSessionQueryable) => Map[K, V]) = collection.extractTo(this)(f)

  @inline protected final def extractFrom[D](dataBoard: DataBoard[D])(f: (D, DBSessionQueryable) => Map[K, V]): Unit = new DataBoard2DataViewExtractor(dataBoard.impl, impl, f)

  private[common] val impl = newDataView[K, V](this)
}

abstract class DataBoard[D] {
  @inline protected final def extractFrom[E <: Entity](collection: EntityCollection[E])(f: (E, DBSessionQueryable) => Seq[D]) = collection.extractTo(this)(f)

  @inline protected final def extractTo[DK, DV](dataView: DataView[DK, DV])(f: (D, DBSessionQueryable) => Map[DK, DV]): Unit = new DataBoard2DataViewExtractor(impl, dataView.impl, f)

  private[common] val impl = new DataBoardImpl[D]
}