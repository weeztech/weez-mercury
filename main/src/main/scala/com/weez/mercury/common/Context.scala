package com.weez.mercury.common

import scala.concurrent._
import EntityCollections._

trait Context {
  implicit val executor: ExecutionContext

  def api: String

  def complete(response: Response): Unit

  def acceptUpload(receiver: UploadContext => Unit): String

  def futureQuery[T](f: DBSessionQueryable => T): Future[T]

  def futureUpdate[T](f: DBSessionUpdatable => T): Future[T]

  def app: Application

  def tempDir: FileIO.Path
}

trait RemoteCallContext extends Context with RangeImplicits {

  import akka.event.LoggingAdapter

  implicit val context: this.type = this

  def response: Response

  def log: LoggingAdapter

  def request: ModelObject

  def peer: String
}

sealed trait Response

sealed trait InstantResponse extends Response

case class FileResponse(path: String) extends InstantResponse

case class ResourceResponse(url: String) extends InstantResponse

case class ModelResponse(obj: ModelObject) extends InstantResponse

case class FutureResponse(x: Future[Response]) extends Response

case class FailureResponse(ex: Throwable) extends InstantResponse

trait UploadContext extends Context {

  import akka.util.ByteString

  def id: String

  def content: Pipe.Readable[ByteString]

  def sessionState: Option[SessionState]
}

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

final case class KeyValue[K, V](key: K, value: V)

trait View[K, V] {

  import com.weez.mercury.macros.TuplePrefixed

  def apply()(implicit db: DBSessionQueryable): Cursor[KeyValue[K, V]]

  def apply[P: Packer](range: Range[P], forward: Boolean = true)(implicit db: DBSessionQueryable, canUse: TuplePrefixed[K, P]): Cursor[KeyValue[K, V]]
}

trait UniqueView[K, V] extends View[K, V] {
  self =>
  def apply(key: K)(implicit db: DBSessionQueryable): Option[V]

  def contains(key: K)(implicit db: DBSessionQueryable): Boolean
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

  protected def defUniqueIndex[K: Packer](name: String, getKey: V => K): UniqueView[K, V]

  protected def defUniqueIndexEx[K: Packer](name: String)(getKey: (V, DBSessionQueryable) => Set[K]): UniqueView[K, V]

  protected def defIndex[K: Packer](name: String, getKey: V => K): View[K, V]

  protected def defIndexEx[K: Packer](name: String)(getKeys: (V, DBSessionQueryable) => Set[K]): View[K, V]
}

object Collections {

  def entity(id: Long)(implicit db: DBSessionQueryable): Option[Entity] = getEntityO[Entity](id)

  def scanFX(word: String)(implicit db: DBSessionQueryable): Cursor[Entity] = FullTextSearchIndex.scan(word, null.asInstanceOf[RootCollectionImpl[Entity]])

  def scanRefers(id: Long)(implicit db: DBSessionQueryable): Cursor[Entity] = RefReverseIndex.scan(id, null.asInstanceOf[RootCollectionImpl[Entity]])
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

  @inline protected final def defUniqueIndex[K: Packer](name: String, getKey: V => K): UniqueView[K, V] = impl.defUniqueIndex(name, getKey)

  @inline protected final def defUniqueIndexEx[K: Packer](name: String)(getKey: (V, DBSessionQueryable) => Set[K]): UniqueView[K, V] = impl.defUniqueIndexEx(name, getKey)

  @inline protected final def defIndexEx[K: Packer](name: String)(getKeys: (V, DBSessionQueryable) => Set[K]): View[K, V] = impl.defIndexEx[K](name, getKeys)

  @inline protected final def defIndex[K: Packer](name: String, getKey: V => K): View[K, V] = impl.defIndex[K](name, getKey)

  @inline protected[common] final def extractTo[D](dataBoard: DataBoard[D])(f: (V, DBSessionQueryable) => Seq[D]): Unit = new Entity2DataBoardExtractor(impl, dataBoard.impl, f)

  @inline protected[common] final def extractTo[DK, DV](dataView: DataView[DK, DV])(f: (V, DBSessionQueryable) => Map[DK, DV]): Unit = new Entity2DataViewExtractor(impl, dataView.impl, f)

  private[common] val impl = RootCollectionImpl[V](this)
}

trait FullTextSupport {
  self: RootCollection[_] =>
}

trait CanMerge[Repr <: CanMerge[Repr]] extends Equals {
  def isEmpty: Boolean

  def +(x: Repr): Repr

  def -(x: Repr): Repr

  def neg(): Repr
}

import scala.reflect.runtime.universe._

@collect
abstract class DataView[K: Packer, V: Packer : TypeTag] extends UniqueView[K, V] {

  import com.weez.mercury.macros.TuplePrefixed

  @inline final def apply(key: K)(implicit db: DBSessionQueryable): Option[V] = impl.get(key)

  @inline final def contains(key: K)(implicit db: DBSessionQueryable): Boolean = impl.contains(key)

  @inline final def apply()(implicit db: DBSessionQueryable): Cursor[KeyValue[K, V]] = impl()

  @inline final def apply[P: Packer](range: Range[P], forward: Boolean = true)(implicit db: DBSessionQueryable, canUse: TuplePrefixed[K, P]): Cursor[KeyValue[K, V]] = impl(range, forward)


  @inline protected final def extractFrom[E <: Entity](collection: RootCollection[E])(f: (E, DBSessionQueryable) => Map[K, V]) = collection.extractTo(this)(f)

  @inline protected final def extractFrom[D](dataBoard: DataBoard[D])(f: (D, DBSessionQueryable) => Seq[(K, V)]): Unit = new DataBoard2DataViewExtractor(dataBoard.impl, impl, f)

  private[common] val impl = DataViewImpl[K, V](this)
}

trait NoMergeInDB {
  self: DataView[_, _] =>
}

abstract class DataBoard[D] {
  @inline protected final def extractFrom[E <: Entity](collection: RootCollection[E])(f: (E, DBSessionQueryable) => Seq[D]) = collection.extractTo(this)(f)

  @inline protected final def extractTo[DK, DV](dataView: DataView[DK, DV])(f: (D, DBSessionQueryable) => Seq[(DK, DV)]): Unit = new DataBoard2DataViewExtractor(impl, dataView.impl, f)

  private[common] val impl = new DataBoardImpl[D]
}