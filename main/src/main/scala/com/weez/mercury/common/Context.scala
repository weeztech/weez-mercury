package com.weez.mercury.common

import akka.event.LoggingAdapter

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
}

trait DBSessionUpdatable extends DBSessionQueryable {
  def put[K: Packer, V: Packer](key: K, value: V): Unit

  def del[K: Packer](key: K): Unit
}

trait Cursor[+T <: Entity] extends Iterator[T] {
  //override def drop(n: Int): Cursor[T]
  def close(): Unit
}

trait KVCollection[K, V <: Entity] {
  @inline final def apply()(implicit db: DBSessionQueryable): Cursor[V] =
    this.apply(None, None)

  @inline final def apply(start: K, end: K)(implicit db: DBSessionQueryable): Cursor[V] =
    this.apply(Some(start), Some(end))

  def apply(start: Option[K], end: Option[K], excludeStart: Boolean = false, excludeEnd: Boolean = false)(implicit db: DBSessionQueryable): Cursor[V]

  def delete(key: K)(implicit db: DBSessionUpdatable): Unit
}

trait KVMap[K, V <: Entity] extends KVCollection[K, V] {
  def apply(key: K)(implicit db: DBSessionQueryable): Option[V]
}

trait IndexBase[K, V <: Entity] extends KVCollection[K, V]

trait Index[K, V <: Entity] extends IndexBase[K, V] {
  def apply(key: K)(implicit db: DBSessionQueryable): Cursor[V]
}

trait UniqueIndex[K, V <: Entity] extends IndexBase[K, V] with KVMap[K,V] {
  def apply(key: K)(implicit db: DBSessionQueryable): Option[V]
}

trait ExtendIndex[K, B <: Entity, V <: ExtendEntity[B]] extends UniqueIndex[K, V]

trait Ref[+T <: Entity] {
  def isEmpty: Boolean

  def apply()(implicit db: DBSessionQueryable): T
}

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


trait Entity {
  val id: Long
}

trait ExtendEntity[B <: Entity] extends Entity {
  //def base: Ref[B]
}


trait KeyCollection[T <: Entity] extends KVMap[Long, T] {

  def update(value: T)(implicit db: DBSessionUpdatable): Unit

  def defUniqueIndex[K: Packer](name: String, keyGetter: T => K): UniqueIndex[K, T]
}


abstract class RootCollection[T <: Entity : Packer] extends KeyCollection[T] {
  rootCollection =>

  val name: String

  val keyPrefix = name

  @inline final private[common] def fullKey(id: Long) = (keyPrefix, id)

  @inline final private[common] def fullKey(v: T): (String, Long) = this.fullKey(v.id)


  private val indexes = collection.mutable.Map[String, IndexBaseImpl[_, _]]()

  /**
   * 内部索引和外部的Extend索引
   */
  private var idxUpdaters = Seq[IndexEntryUpdater[T]]()
  private var idxInserters = Seq[IndexEntryInserter[T]]()
  private var idxDeleters = Seq[IndexEntryDeleter[T]]()

  protected[common] def registerIndexEntryHelper(er: IndexEntryHelper[T]): Unit = {
    er match {
      case er: IndexEntryUpdater[T] =>
        this.idxUpdaters :+= er
      case _ =>
    }
    er match {
      case er: IndexEntryInserter[T] =>
        this.idxInserters :+= er
      case _ =>
    }
    er match {
      case er: IndexEntryDeleter[T] =>
        this.idxDeleters :+= er
      case _ =>
    }
  }

  private def newIndex(name: String, index: IndexBaseImpl[_, _]): String = {
    this.indexes.put(name, index) match {
      case Some(old: IndexBaseImpl[_, T]) =>
        this.indexes.put(name, old)
        throw new IllegalArgumentException(s"index with name [$name] already exists!")
      case _ =>
        index match {
          case ui: IndexEntryHelper[T] =>
            this.registerIndexEntryHelper(ui)
          case _ =>
        }
        this.keyPrefix + '.' + name
    }
  }

  override def apply(start: Option[Long], end: Option[Long], excludeStart: Boolean, excludeEnd: Boolean)(implicit db: DBSessionQueryable): Cursor[T] = {
    new CursorImpl[String, Long, T, T](this.keyPrefix, start, end, excludeStart, excludeEnd, v => v)
  }

  override final def apply(key: Long)(implicit db: DBSessionQueryable): Option[T] = db.get(this.fullKey(key))

  override final def update(value: T)(implicit db: DBSessionUpdatable): Unit = {
    val key = this.fullKey(value)
    if (!(this.idxUpdaters.isEmpty && this.idxInserters.isEmpty)) {
      //有索引，需要更新索引
      db.get(key) match {
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
    db.put(key, value)
  }

  final def delete(id: Long)(implicit db: DBSessionUpdatable): Unit = {
    val key = this.fullKey(id)
    if (this.idxDeleters.isEmpty) {
      db.del(key)
    } else {
      //有索引，需要更新索引
      db.get(key) match {
        case Some(old) =>
          for (i <- this.idxDeleters) {
            i.onKeyEntityDelete(old)
          }
          db.del(key)
        case _ =>
      }
    }
  }


  private[common] abstract class IndexBaseImpl[K: Packer, KB <: Entity](name: String, keyGetter: KB => K) extends KVCollection[K, T] with UniqueIndex[K, T] {
    val keyPrefix = rootCollection.newIndex(name, this)

    def getImplicit[I](implicit i: I) = i

    val fullKeyPacker = Packer.tuple2(Packer.StringPacker, getImplicit[Packer[K]])


    @inline final def getRefID(key: K)(implicit db: DBSessionQueryable) = db.get(this.fullKey(key))(fullKeyPacker, Packer.LongPacker)

    override final def apply(key: K)(implicit db: DBSessionQueryable): Option[T] = {
      val id: Option[Long] = this.getRefID(key)
      id match {
        case Some(i) => rootCollection(i)
        case _ => None
      }
    }

    override final def delete(key: K)(implicit db: DBSessionUpdatable): Unit = {
      rootCollection.delete(this.getRefID(key).get)
    }

    override final def apply(start: Option[K], end: Option[K], excludeStart: Boolean, excludeEnd: Boolean)(implicit db: DBSessionQueryable): Cursor[T] = {
      new CursorImpl[String, K, Long, T](this.keyPrefix, start, end, excludeStart, excludeEnd, rootCollection(_).get)
    }

    @inline final private[common] def fullKey(key: K): (String, K) = this.fullKey(key)

    @inline final private[common] def fullKey(keyEntity: KB): (String, K) = this.fullKey(keyGetter(keyEntity))

    @inline final def doUpdateEntry(oldKeyEntity: KB, newKeyEntity: KB, condition: () => Boolean)(implicit db: DBSessionUpdatable): Unit = {
      val oldIndexEntryKey = keyGetter(oldKeyEntity)
      val newIndexEntryKey = keyGetter(newKeyEntity)
      if (!oldIndexEntryKey.equals(newIndexEntryKey) && condition()) {
        db.del(this.fullKey(oldIndexEntryKey))
        db.put(this.fullKey(newIndexEntryKey), newKeyEntity.id)
      }
    }
  }

  private class UniqueIndexImpl[K: Packer](name: String, keyGetter: T => K)
    extends IndexBaseImpl[K, T](name, keyGetter)
    with UniqueIndex[K, T]
    with IndexEntryInserter[T]
    with IndexEntryDeleter[T]
    with IndexEntryUpdater[T] {

    def onKeyEntityInsert(newEntity: T)(implicit db: DBSessionUpdatable): Unit = {
      db.put(this.fullKey(newEntity), newEntity.id)
    }

    def onKeyEntityDelete(oldEntity: T)(implicit db: DBSessionUpdatable): Unit = {
      db.del(this.fullKey(oldEntity))
    }

    def onKeyEntityUpdate(oldEntity: T, newEntity: T)(implicit db: DBSessionUpdatable) = {
      this.doUpdateEntry(oldEntity, newEntity, () => true)
    }
  }

  final def defUniqueIndex[K: Packer](name: String, keyGetter: T => K): UniqueIndex[K, T] = new UniqueIndexImpl[K](name, keyGetter)

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
      lazy val exists = db.exists(self.fullKey(id))
      for (ei <- this.eis) {
        ei.doUpdateEntry(oldEntity, newEntity, () => exists)
      }
    }

    def defExtendIndex[K: Packer](name: String, keyGetter: B => K): ExtendIndex[K, B, T] = {
      val ei = new ExtendIndexImpl(name, keyGetter)
      if (this.eis.isEmpty) {
        self.base.registerIndexEntryHelper(this)
      }
      this.eis :+= ei
      ei
    }
  }

  private[common] class ExtendIndexImpl[K: Packer](name: String, keyGetter: B => K)
    extends IndexBaseImpl[K, B](name, keyGetter)
    with ExtendIndex[K, B, T]
    with IndexEntryInserter[T]
    with IndexEntryDeleter[T] {


    def onKeyEntityDelete(oldEntity: T)(implicit db: DBSessionUpdatable): Unit = {
      db.del(this.fullKey(self.base(oldEntity.id).get))
    }

    def onKeyEntityInsert(newEntity: T)(implicit db: DBSessionUpdatable): Unit = {
      val id = newEntity.id
      db.put(this.fullKey(self.base(id).get), id)
    }
  }

  final def defExtendIndex[K: Packer](name: String, keyGetter: B => K): ExtendIndex[K, B, T] =
    this.ExtendIndexes.defExtendIndex(name, keyGetter)
}


private[common] trait IndexEntryHelper[KB] {
}

private[common] trait IndexEntryInserter[KB] extends IndexEntryHelper[KB] {
  def onKeyEntityInsert(newEntity: KB)(implicit db: DBSessionUpdatable)
}

private[common] trait IndexEntryUpdater[KB] extends IndexEntryHelper[KB] {
  def onKeyEntityUpdate(oldEntity: KB, newEntity: KB)(implicit db: DBSessionUpdatable)
}

private[common] trait IndexEntryDeleter[KB] extends IndexEntryHelper[KB] {
  def onKeyEntityDelete(oldEntity: KB)(implicit db: DBSessionUpdatable)
}


private[common] class CursorImpl[PF: Packer, K: Packer, V: Packer, T <: Entity](prefix: PF, keyStart: Option[K], keyEnd: Option[K],
                                                                                excludeStart: Boolean, excludeEnd: Boolean, v2t: V => T)
                                                                               (implicit db: DBSessionQueryable) extends Cursor[T] {

  private var dbCursor: DBCursor[(PF, K), T] = null

  def next(): T = ???

  def hasNext = dbCursor.hasNext

  def close() = {
    if (dbCursor != null) {
      dbCursor.close()
    }
  }
}
