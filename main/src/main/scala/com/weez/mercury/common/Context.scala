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

trait IndexBase[K, V <: Entity] {
  @inline final def apply()(implicit db: DBSessionQueryable): Cursor[V] =
    this.apply(None, None)

  @inline final def apply(start: K, end: K)(implicit db: DBSessionQueryable): Cursor[V] =
    this.apply(Some(start), Some(end))

  def apply(start: Option[K], end: Option[K], excludeStart: Boolean = false, excludeEnd: Boolean = false)(implicit db: DBSessionQueryable): Cursor[V]
}

trait Index[K, V <: Entity] extends IndexBase[K, V]

trait UniqueIndex[K, V <: Entity] extends IndexBase[K, V] {
  def apply(key: K)(implicit db: DBSessionQueryable): Option[V]

  def delete(key: K)(implicit db: DBSessionUpdatable): Unit

  def update(value: V)(implicit db: DBSessionUpdatable): Unit
}

trait Ref[+T <: Entity] {
  def isEmpty: Boolean

  def refID: Long

  @inline final def isDefined = !this.isEmpty

  def apply()(implicit db: DBSessionQueryable): T
}

trait Entity {
  val id: Long
}

trait ExtendEntity[B <: Entity] extends Entity {
  //def base: Ref[B]
}

trait SubEntity[-P <: Entity] extends Entity {
  def parentID: Long
}

trait EntityCollection[V <: Entity] {
  //add or modify
  def update(value: V)(implicit db: DBSessionUpdatable): Unit

  def delete(value: V)(implicit db: DBSessionUpdatable): Unit
}

trait PartitionCollection[P <: Entity, V <: SubEntity[P]] extends EntityCollection[V] {
  def host: P

  def partitionOf: SubCollection[P, V]

  @inline private def checkHost(value: V): Unit = {
    if (value.parentID != host.id) {
      throw new IllegalArgumentException("not in this host")
    }
  }

  @inline final override def update(value: V)(implicit db: DBSessionUpdatable): Unit = {
    this.checkHost(value)
    this.partitionOf.update(value)
  }

  @inline final override def delete(value: V)(implicit db: DBSessionUpdatable): Unit = {
    this.checkHost(value)
    this.partitionOf.delete(value.id)
  }

  @inline final def defUniqueIndex[K](pIndex: PartitionIndex[P, K, V]): UniqueIndex[K, V] = pIndex.newInlineIndex(host)
}

trait HostCollection[V <: Entity] extends EntityCollection[V] with UniqueIndex[Long, V] {
  host =>
  val name: String
  private var collectionID: Int = 0

  private def bindDB()(implicit db: DBSessionQueryable): Unit = {
    this.synchronized {
      val meta = db.schema.getHostCollection(this.name)
      this.collectionID = meta.id
      for (idx <- indexes.values) {
        idx.indexID = meta.indexes.find(i => i.name == idx.name).get.id
      }
    }
  }

  implicit val valuePacker: Packer[V]

  @inline private[common] final def cid(implicit db: DBSessionQueryable) = {
    if (this.collectionID == 0) {
      this.bindDB()
    }
    this.collectionID
  }

  @inline private[common] final def getImplicit[I](implicit p: I) = p

  @inline private[common] final def getImplicitPacker[I](implicit p: Packer[I]) = p

  private[common] abstract class IndexBaseImpl[K: Packer, KB <: Entity](keyGetter: KB => K)
    extends UniqueIndex[K, V] {
    val name: String
    var indexID = 0

    host.onNewIndex(this)

    def getIndexID(implicit db: DBSessionQueryable) = {
      if (this.indexID == 0) {
        host.bindDB()
      }
      this.indexID
    }

    final val fullKeyPacker = getImplicitPacker[(Int, K)]

    @inline final def getFullKey(key: K)(implicit db: DBSessionQueryable): (Int, K) = (this.getIndexID, key)

    @inline final def getFullKeyOfKB(keyBase: KB)(implicit db: DBSessionQueryable): (Int, K) = this.getFullKey(keyGetter(keyBase))

    @inline final def getRefID(key: K)(implicit db: DBSessionQueryable): Option[Long] = db.get(getFullKey(key))(fullKeyPacker, getImplicitPacker[Long])

    override final def apply(key: K)(implicit db: DBSessionQueryable): Option[V] = {
      this.getRefID(key).flatMap(db.get[Long, V])
    }

    override final def delete(key: K)(implicit db: DBSessionUpdatable): Unit = {
      this.getRefID(key).foreach(host.delete)
    }

    override final def apply(start: Option[K], end: Option[K], excludeStart: Boolean, excludeEnd: Boolean)(implicit db: DBSessionQueryable): Cursor[V] = {
      new CursorImpl[K, Long, V](this.getIndexID, start, end, excludeStart, excludeEnd, id => db.get[Long, V](id).get)
    }

    override def update(value: V)(implicit db: DBSessionUpdatable): Unit = {
      host.update(value)
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

  private[common] class UniqueIndexImpl[K: Packer](override val name: String, keyGetter: V => K)
    extends IndexBaseImpl[K, V](keyGetter)
    with UniqueIndex[K, V]
    with IndexEntryInserter[V]
    with IndexEntryDeleter[V]
    with IndexEntryUpdater[V] {

    def onKeyEntityInsert(newEntity: V)(implicit db: DBSessionUpdatable): Unit = {
      db.put(this.getFullKeyOfKB(newEntity), newEntity.id)(this.fullKeyPacker, getImplicitPacker[Long])
    }

    def onKeyEntityDelete(oldEntity: V)(implicit db: DBSessionUpdatable): Unit = {
      db.del(this.getFullKeyOfKB(oldEntity))(this.fullKeyPacker)
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

  private def onNewIndex(index: IndexBaseImpl[_, _]) = {
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
    }
  }

  @inline private[common] final def fixID(id: Long)(implicit db: DBSessionQueryable) = (id & 0xFFFFFFFFFFFFFFL) | (this.cid.asInstanceOf[Long] << 48)

  @inline private[common] final def fixIDAndGet(id: Long)(implicit db: DBSessionQueryable): Option[V] = db.get(this.fixID(id))

  @inline final def newID()(implicit db: DBSessionUpdatable) = this.fixID(db.schema.newEntityID())


  @inline override final def apply(id: Long)(implicit db: DBSessionQueryable): Option[V] = {
    if (id == 0) {
      None
    } else if ((id >>> 48) != this.cid) {
      throw new IllegalArgumentException("not in this collection")
    } else {
      db.get[Long, V](id)
    }
  }

  override final def apply(start: Option[Long], end: Option[Long], excludeStart: Boolean, excludeEnd: Boolean)(implicit db: DBSessionQueryable): Cursor[V] =
    new CursorImpl[Long, V, V](this.cid, start, end, excludeStart, excludeEnd, v => v)


  override final def update(value: V)(implicit db: DBSessionUpdatable): Unit = {
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

  override final def delete(id: Long)(implicit db: DBSessionUpdatable): Unit = {
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


  override def delete(value: V)(implicit db: DBSessionUpdatable): Unit = {
    this.delete(value.id)
  }

  def defUniqueIndex[K: Packer](name: String, keyGetter: V => K): UniqueIndex[K, V] = new UniqueIndexImpl[K](name, keyGetter)
}

trait ExtendCollection[B <: Entity, V <: ExtendEntity[B]] extends HostCollection[V] {
  host =>
  def extendFrom: HostCollection[B]

  private[common] object ExtendIndexes extends IndexEntryDeleter[B] with IndexEntryUpdater[B] {
    private var eis = Seq[ExtendIndexImpl[_]]()

    def onKeyEntityDelete(oldEntity: B)(implicit db: DBSessionUpdatable): Unit = {
      host.delete(oldEntity.id)
    }

    def onKeyEntityUpdate(oldEntity: B, newEntity: B)(implicit db: DBSessionUpdatable): Unit = {
      val id = oldEntity.id
      lazy val exists = db.exists(id)
      for (ei <- this.eis) {
        ei.doUpdateEntry(oldEntity, newEntity, () => exists)
      }
    }

    def registerExtendIndex[I <: ExtendIndexImpl[_]](idx: I): I = {
      if (this.eis.isEmpty) {
        host.extendFrom.registerIndexEntryHelper(this)
      }
      this.eis :+= idx
      idx
    }
  }

  private[common] class ExtendIndexImpl[K: Packer](override val name: String, keyGetter: B => K)
    extends IndexBaseImpl[K, B](keyGetter)
    with UniqueIndex[K, V]
    with IndexEntryInserter[V]
    with IndexEntryDeleter[V] {


    @inline final def getFullKey(entity: V)(implicit db: DBSessionUpdatable): (Int, K) = this.getFullKeyOfKB(host.extendFrom.fixIDAndGet(entity.id).get)

    def onKeyEntityDelete(oldEntity: V)(implicit db: DBSessionUpdatable): Unit = {
      db.del(this.getFullKey(oldEntity))
    }

    def onKeyEntityInsert(newEntity: V)(implicit db: DBSessionUpdatable): Unit = {
      db.put(this.getFullKey(newEntity), newEntity.id)
    }
  }

  @inline final def defExtendIndex[K: Packer](name: String, keyGetter: B => K): UniqueIndex[K, V] =
    this.ExtendIndexes.registerExtendIndex(new ExtendIndexImpl[K](name, keyGetter))
}

trait PartitionIndex[P <: Entity, K, V <: SubEntity[P]] extends UniqueIndex[(Long, K), V] {

  private class InlineIndex(belongID: Long, baseIndex: UniqueIndex[(Long, K), V]) extends UniqueIndex[K, V] {

    override def apply(key: K)(implicit db: DBSessionQueryable): Option[V] = baseIndex((belongID, key))

    override def apply(start: Option[K], end: Option[K], excludeStart: Boolean, excludeEnd: Boolean)(implicit db: DBSessionQueryable): Cursor[V] =
      baseIndex(start.map((belongID, _)), end.map((belongID, _)), excludeStart, excludeEnd)

    override def delete(key: K)(implicit db: DBSessionUpdatable): Unit = baseIndex.delete((belongID, key))

    override def update(value: V)(implicit db: DBSessionUpdatable): Unit = {
      if (value.parentID != belongID) {
        throw new IllegalArgumentException
      }
      baseIndex.update(value)
    }
  }

  @inline final def newInlineIndex(belong: P): UniqueIndex[K, V] = new InlineIndex(belong.id, this)

  @inline final def apply(belong: P) = this.newInlineIndex(belong)
}


trait SubCollection[P <: Entity, V <: SubEntity[P]]{
  self: HostCollection[V] =>
  val parent: HostCollection[P]

  private class PartitionIndexImpl[K: Packer](name: String, keyGetter: V => K)
    extends UniqueIndexImpl[(Long, K)](name, v => (v.parentID, keyGetter(v)))
    with PartitionIndex[P, K, V]

  def defPartitionIndex[K: Packer](name: String, keyGetter: V => K): PartitionIndex[P, K, V] =
    new PartitionIndexImpl[K](name, keyGetter)
}

trait SubExtendCollection[P <: Entity, B <: SubEntity[P], V <: SubEntity[P] with ExtendEntity[B]]
  extends SubCollection[P, V]
  with ExtendCollection[B, V] {

  private[common] class PartitionExtendIndexImpl[K: Packer](name: String, keyGetter: B => K)
    extends ExtendIndexImpl[(Long, K)](name, b => (b.parentID, keyGetter(b)))
    with PartitionIndex[P, K, V]

  def defPartitionExtendIndex[K: Packer](name: String, keyGetter: B => K): PartitionIndex[P, K, V] =
    this.ExtendIndexes.registerExtendIndex(new PartitionExtendIndexImpl[K](name, keyGetter))
}

trait KeyCollection[T <: Entity] {

  def update(value: T)(implicit db: DBSessionUpdatable): Unit

  def defUniqueIndex[K: Packer](name: String, keyGetter: T => K): UniqueIndex[K, T]
}

abstract class RootCollection[V <: Entity : Packer] extends HostCollection[V] {
  define =>
  override val valuePacker = getImplicitPacker[V]
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
