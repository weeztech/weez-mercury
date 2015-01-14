package com.weez.mercury.common

import akka.event.LoggingAdapter

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

  def newCursor[K: Packer, V: Packer]: DBCursor[K, V]

  private[common] def schema: DBSchema
}

trait DBSessionUpdatable extends DBSessionQueryable {
  def put[K: Packer, V: Packer](key: K, value: V): Unit

  def del[K: Packer](key: K): Unit
}

trait Cursor[+T <: Entity] extends Iterator[T] {
  //override def drop(n: Int): Cursor[T]
  def close(): Unit
}

trait OrderedKV[K, V <: Entity] {
  @inline final def apply()(implicit db: DBSessionQueryable): Cursor[V] =
    this.apply(None, None)

  @inline final def apply(start: K, end: K)(implicit db: DBSessionQueryable): Cursor[V] =
    this.apply(Some(start), Some(end))

  def apply(start: Option[K], end: Option[K], excludeStart: Boolean = false, excludeEnd: Boolean = false)(implicit db: DBSessionQueryable): Cursor[V]
}

trait UniqueKV[K, V <: Entity] {
  def apply(key: K)(implicit db: DBSessionQueryable): Option[V]

  def delete(key: K)(implicit db: DBSessionUpdatable): Unit
}

trait IndexBase[K, V <: Entity] extends OrderedKV[K, V]

trait Index[K, V <: Entity] extends IndexBase[K, V]

trait UniqueIndex[K, V <: Entity] extends IndexBase[K, V] with UniqueKV[K, V]

trait Ref[+T <: Entity] {
  def isEmpty: Boolean

  @inline final def isDefined = !this.isEmpty

  def apply()(implicit db: DBSessionQueryable): T
}

trait Entity {
  val id: Long
}

trait ExtendEntity[B <: Entity] extends Entity {
  //def base: Ref[B]
}


trait EntityCollection[T <: Entity] {
  def update(value: T)(implicit db: DBSessionUpdatable): Unit
}

trait PartitionCollection[T <: Entity] extends EntityCollection[T] {

}

trait HostCollection[T <: Entity] extends EntityCollection[T] {

}

trait KeyCollection[T <: Entity] extends OrderedKV[Long, T] with UniqueKV[Long, T] {

  def update(value: T)(implicit db: DBSessionUpdatable): Unit

  def defUniqueIndex[K: Packer](name: String, keyGetter: T => K): UniqueIndex[K, T]
}

trait UniqueOrderedKV[K, V <: Entity] extends OrderedKV[K, V] with UniqueKV[K, V] {
  val name: String
  private[common] val fullName: String = name
  private var _cid: Int = 0

  @inline private[common] final def cid(implicit db: DBSessionQueryable) = {
    if (this._cid == 0) {
      this._cid = db.schema.getHostCollection(this.fullName).id
      ???
    }
    this._cid
  }

  @inline private[common] final def getImplicitPacker[I](implicit p: Packer[I]) = p
}

abstract class RootCollection[V <: Entity : Packer] extends UniqueOrderedKV[Long, V] {
  define =>

  private[common] abstract class IndexBaseImpl[K: Packer, KB <: Entity](keyGetter: KB => K)
    extends UniqueOrderedKV[K, V] {

    override private[common] val fullName: String = define.onNewIndex(this)

    final val fullKeyPacker = getImplicitPacker[(Int, K)]

    @inline final def getFullKey(key: K)(implicit db: DBSessionQueryable): (Int, K) = (this.cid, key)

    @inline final def getFullKey(keyBase: KB)(implicit db: DBSessionQueryable): (Int, K) = this.getFullKey(keyGetter(keyBase))

    @inline final def getRefID(key: K)(implicit db: DBSessionQueryable): Option[Long] = db.get(getFullKey(key))(fullKeyPacker, getImplicitPacker[Long])

    @inline final def apply(key: K)(implicit db: DBSessionQueryable): Option[V] = {
      this.getRefID(key).flatMap(db.get[Long, V])
    }

    @inline final def delete(key: K)(implicit db: DBSessionUpdatable): Unit = {
      this.getRefID(key).foreach(define.delete)
    }

    @inline final def apply(start: Option[K], end: Option[K], excludeStart: Boolean, excludeEnd: Boolean)(implicit db: DBSessionQueryable): Cursor[V] = {
      new CursorImpl[K, Long, V](this.cid, start, end, excludeStart, excludeEnd, id => db.get[Long, V](id).get)
    }

    @inline final def doUpdateEntry(oldKeyEntity: KB, newKeyEntity: KB, condition: () => Boolean)(implicit db: DBSessionUpdatable): Unit = {
      val oldIndexEntryKey = keyGetter(oldKeyEntity)
      val newIndexEntryKey = keyGetter(newKeyEntity)
      if (!oldIndexEntryKey.equals(newIndexEntryKey) && condition()) {
        db.del(this.getFullKey(oldIndexEntryKey))
        db.put(this.getFullKey(newIndexEntryKey), newKeyEntity.id)
      }
    }
  }

  private class UniqueIndexImpl[K: Packer](override val name: String, keyGetter: V => K)
    extends IndexBaseImpl[K, V](keyGetter)
    with UniqueIndex[K, V]
    with IndexEntryInserter[V]
    with IndexEntryDeleter[V]
    with IndexEntryUpdater[V] {

    def onKeyEntityInsert(newEntity: V)(implicit db: DBSessionUpdatable): Unit = {
      db.put(this.getFullKey(newEntity), newEntity.id)(this.fullKeyPacker, this.getImplicitPacker[Long])
    }

    def onKeyEntityDelete(oldEntity: V)(implicit db: DBSessionUpdatable): Unit = {
      db.del(this.getFullKey(oldEntity))(this.fullKeyPacker)
    }

    def onKeyEntityUpdate(oldEntity: V, newEntity: V)(implicit db: DBSessionUpdatable): Unit = {
      this.doUpdateEntry(oldEntity, newEntity, () => true)
    }
  }


  private val indexes = collection.mutable.Map[String, IndexBaseImpl[_, _]]()

  /**
   * 内部索引和外部的Extend索引
   */
  private var idxUpdaters = Seq[IndexEntryUpdater[V]]()
  private var idxInserters = Seq[IndexEntryInserter[V]]()
  private var idxDeleters = Seq[IndexEntryDeleter[V]]()

  private[common] def registerIndexEntryHelper(er: IndexEntryHelper[V]): Unit = {
    er match {
      case er: IndexEntryUpdater[V] =>
        this.idxUpdaters :+= er
      case _ =>
    }
    er match {
      case er: IndexEntryInserter[V] =>
        this.idxInserters :+= er
      case _ =>
    }
    er match {
      case er: IndexEntryDeleter[V] =>
        this.idxDeleters :+= er
      case _ =>
    }
  }

  private def onNewIndex(index: IndexBaseImpl[_, _]): String = {
    this.indexes.put(index.name, index) match {
      case Some(old: IndexBaseImpl[_, V]) =>
        this.indexes.put(index.name, old)
        throw new IllegalArgumentException(s"index with name [${index.name}] already exists!")
      case _ =>
        index match {
          case ui: IndexEntryHelper[V] =>
            this.registerIndexEntryHelper(ui)
          case _ =>
        }
        this.name + '.' + index.name
    }
  }

  @inline private[common] final def fixID(id: Long)(implicit db: DBSessionQueryable) = (id & 0xFFFFFFFFFFFFFFL) | (this.cid.asInstanceOf[Long] << 48)

  @inline private[common] final def fixIDAndGet(id: Long)(implicit db: DBSessionQueryable): Option[V] = db.get(this.fixID(id))

  @inline final def newID()(implicit db: DBSessionUpdatable) = this.fixID(db.schema.newEntityID())


  @inline final def apply(id: Long)(implicit db: DBSessionQueryable): Option[V] = {
    if (id == 0) {
      None
    } else if ((id >>> 48) != this.cid) {
      throw new IllegalArgumentException("not in this collection")
    } else {
      db.get[Long, V](id)
    }
  }

  final def apply(start: Option[Long], end: Option[Long], excludeStart: Boolean, excludeEnd: Boolean)(implicit db: DBSessionQueryable): Cursor[V] =
    new CursorImpl[Long, V, V](this.cid, start, end, excludeStart, excludeEnd, v => v)


  final def update(value: V)(implicit db: DBSessionUpdatable): Unit = {
    val id = value.id
    if (!(this.idxUpdaters.isEmpty && this.idxInserters.isEmpty)) {
      //有索引，需要更新索引
      db.get(id) match {
        case Some(old) =>
          for (i <- this.idxUpdaters) {
            i.onKeyEntityUpdate(old, value)
          }
        case _ =>
          for (i <- this.idxInserters) {
            i.onKeyEntityInsert(value)
          }
      }
    }
    db.put(id, value)
  }

  final def delete(id: Long)(implicit db: DBSessionUpdatable): Unit = {
    if (this.idxDeleters.isEmpty) {
      db.del(id)
    } else {
      //有索引，需要更新索引
      db.get(id).foreach { old =>
        for (i <- this.idxDeleters) {
          i.onKeyEntityDelete(old)
        }
        db.del(id)
      }
    }
  }

  def defUniqueIndex[K: Packer](name: String, keyGetter: V => K): UniqueIndex[K, V] = new UniqueIndexImpl[K](name, keyGetter)
}

abstract class ExtendRootCollection[T <: ExtendEntity[B] : Packer, B <: Entity : Packer] extends RootCollection[T] {
  self =>
  def base: RootCollection[B]

  private object ExtendIndexes extends IndexEntryDeleter[B] with IndexEntryUpdater[B] {
    private var eis = Seq[ExtendIndexImpl[_]]()

    def onKeyEntityDelete(oldEntity: B)(implicit db: DBSessionUpdatable): Unit = {
      self.delete(oldEntity.id)
    }

    def onKeyEntityUpdate(oldEntity: B, newEntity: B)(implicit db: DBSessionUpdatable): Unit = {
      val id = oldEntity.id
      lazy val exists = db.exists(id)
      for (ei <- this.eis) {
        ei.doUpdateEntry(oldEntity, newEntity, () => exists)
      }
    }

    def defExtendIndex[K: Packer](name: String, keyGetter: B => K): ExtendIndexImpl[K] = {
      val ei = new ExtendIndexImpl(name, keyGetter)
      if (this.eis.isEmpty) {
        self.base.registerIndexEntryHelper(this)
      }
      this.eis :+= ei
      ei
    }
  }

  private[common] class ExtendIndexImpl[K: Packer](override val name: String, keyGetter: B => K)
    extends IndexBaseImpl[K, B](keyGetter)
    with UniqueIndex[K, T]
    with IndexEntryInserter[T]
    with IndexEntryDeleter[T] {


    @inline final def getFullKey(entity: T)(implicit db: DBSessionUpdatable): (Int, K) = this.getFullKey(self.base.fixIDAndGet(entity.id).get)

    def onKeyEntityDelete(oldEntity: T)(implicit db: DBSessionUpdatable): Unit = {
      db.del(this.getFullKey(oldEntity))
    }

    def onKeyEntityInsert(newEntity: T)(implicit db: DBSessionUpdatable): Unit = {
      db.put(this.getFullKey(newEntity), newEntity.id)
    }
  }

  final def defExtendIndex[K: Packer](name: String, keyGetter: B => K): UniqueIndex[K, T] =
    this.ExtendIndexes.defExtendIndex(name, keyGetter)
}


private trait IndexEntryHelper[KB]

private trait IndexEntryInserter[KB] extends IndexEntryHelper[KB] {
  def onKeyEntityInsert(newEntity: KB)(implicit db: DBSessionUpdatable)
}

private trait IndexEntryUpdater[KB] extends IndexEntryHelper[KB] {
  def onKeyEntityUpdate(oldEntity: KB, newEntity: KB)(implicit db: DBSessionUpdatable)
}

private trait IndexEntryDeleter[KB] extends IndexEntryHelper[KB] {
  def onKeyEntityDelete(oldEntity: KB)(implicit db: DBSessionUpdatable)
}


private class CursorImpl[K: Packer, V: Packer, T <: Entity](kCID: Int, keyStart: Option[K], keyEnd: Option[K],
                                                            excludeStart: Boolean, excludeEnd: Boolean, v2t: V => T)
                                                           (implicit db: DBSessionQueryable) extends Cursor[T] {

  private var dbCursor: DBCursor[(Int, K), T] = null

  def next(): T = ???

  def hasNext = dbCursor.hasNext

  def close() = {
    if (dbCursor != null) {
      dbCursor.close()
    }
  }
}
