package com.weez.mercury.common

import java.util


private object EntityCollections {


  @inline final def collectionIDOf(entityID: Long): Int = (entityID >>> 48).asInstanceOf[Int]

  @inline final def entityIDOf(collectionID: Int, rawID: Long): Long = (rawID & 0xFFFFFFFFFFFFL) | (collectionID.asInstanceOf[Long] << 48)

  def getEntity[V <: Entity](id: Long)(implicit db: DBSessionQueryable): V = {
    this.getHost(collectionIDOf(id)).apply(id).get.asInstanceOf[V]
  }

  def getHost(collectionID: Int)(implicit db: DBSessionQueryable): HostCollectionImpl[_] = {
    this.synchronized[HostCollectionImpl[_]] {
      this.hostsByID.getOrElse(collectionID, {
        this.hosts.synchronized {
          if (this.hosts.size != this.hostsByID.size) {
            for (h <- this.hosts.values) {
              h.bindDB()
            }
          }
        }
        this.hostsByID.get(collectionID).get
      })
    }
  }

  def forPartitionHost[V <: Entity : Packer](pc: PartitionCollection[_, V]): HostCollection[V] = {
    this.hosts.synchronized {
      this.hosts.get(pc.name) match {
        case Some(host: HostCollectionImpl[_]) => host.asInstanceOf[HostCollectionImpl[V]]
        case _ =>
          val host = new HostCollectionImpl[V](pc.name)
          this.hosts.put(pc.name, host)
          host
      }
    }
  }

  def newHost[V <: Entity : Packer](name: String): HostCollection[V] = {
    this.hosts.synchronized {
      if (this.hosts.get(name).isDefined) {
        throw new IllegalArgumentException( s"""HostCollection naming"$name" exist!""")
      }
      val host = new HostCollectionImpl[V](name)
      this.hosts.put(name, host)
      host
    }
  }

  val hosts = collection.mutable.HashMap[String, HostCollectionImpl[_]]()
  val hostsByID = collection.mutable.HashMap[Int, HostCollectionImpl[_]]()

  class HostCollectionImpl[V <: Entity : Packer](override val name: String) extends HostCollection[V] {
    host =>
    @volatile var meta: DBType.Collection = null
    val valuePacker: Packer[V] = implicitly[Packer[V]]

    def bindDB()(implicit db: DBSessionQueryable): Unit = {
      if (this.synchronized {
        if (this.meta == null) {
          this.meta = db.schema.getHostCollection(this.name)
          if (this.meta == null) {
            throw new IllegalArgumentException( s"""no such HostCollection named ${this.name}""")
          }
          this.indexes.synchronized {
            for (idx <- this.indexes.values) {
              idx.indexID = meta.indexes.find(i => i.name == idx.name).get.id
            }
          }
          true
        } else {
          false
        }
      }) {
        hostsByID.synchronized {
          hostsByID.put(this.meta.id, this)
        }
      }
    }

    @inline final def getMeta(implicit db: DBSessionQueryable) = {
      if (this.meta == null) {
        this.bindDB()
      }
      this.meta
    }

    @inline final def getIndexID(name: String)(implicit db: DBSessionQueryable) = {
      this.getMeta.indexes.find(i => i.name == name).get.id
    }

    abstract class IndexBaseImpl[K: Packer, KB <: Entity](keyGetter: KB => K) extends UniqueIndex[K, V] {

      registerIndexEntryHelper(this)

      val name: String
      var indexID = 0

      def getIndexID(implicit db: DBSessionQueryable) = {
        if (this.indexID == 0) {
          this.indexID = host.getIndexID(this.name)
        }
        this.indexID
      }

      implicit final val fullKeyPacker = implicitly[Packer[(Int, K)]]

      @inline final def getFullKey(key: K)(implicit db: DBSessionQueryable): (Int, K) = (this.getIndexID, key)

      @inline final def getFullKeyOfKB(keyBase: KB)(implicit db: DBSessionQueryable): (Int, K) = this.getFullKey(keyGetter(keyBase))

      @inline final def getRefID(key: K)(implicit db: DBSessionQueryable): Option[Long] = db.get(getFullKey(key))(fullKeyPacker, implicitly[Packer[Long]])

      override final def apply(key: K)(implicit db: DBSessionQueryable): Option[V] = {
        this.getRefID(key).flatMap(db.get[Long, V])
      }

      override final def delete(key: K)(implicit db: DBSessionUpdatable): Unit = {
        this.getRefID(key).foreach(host.delete)
      }

      override final def apply(start: Option[K], end: Option[K], excludeStart: Boolean, excludeEnd: Boolean, forward: Boolean)(implicit db: DBSessionQueryable): Cursor[V] = {
        new CursorImpl[K, Long, V](this.getIndexID, start, end, excludeStart, excludeEnd, if (forward) 1 else -1, id => db.get[Long, V](id).get)
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

    class UniqueIndexImpl[K: Packer](override val name: String, keyGetter: V => K)
      extends IndexBaseImpl[K, V](keyGetter)
      with UniqueIndex[K, V]
      with IndexEntryInserter[V]
      with IndexEntryDeleter[V]
      with IndexEntryUpdater[V] {

      def onKeyEntityInsert(newEntity: V)(implicit db: DBSessionUpdatable): Unit = {
        db.put(this.getFullKeyOfKB(newEntity), newEntity.id)(this.fullKeyPacker, implicitly[Packer[Long]])
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

    private[common] def registerIndexEntryHelper(er: AnyRef): Unit = {
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

    @inline private[common] final def fixID(id: Long)(implicit db: DBSessionQueryable) = entityIDOf(this.getMeta.id, id)

    @inline private[common] final def fixIDAndGet(id: Long)(implicit db: DBSessionQueryable): Option[V] = db.get(this.fixID(id))

    @inline final def newID()(implicit db: DBSessionUpdatable) = this.fixID(db.schema.newEntityID())


    @inline final def apply(id: Long)(implicit db: DBSessionQueryable): Option[V] = {
      if (id == 0) {
        None
      } else if (collectionIDOf(id) != this.getMeta.id) {
        throw new IllegalArgumentException("not in this collection")
      } else {
        db.get[Long, V](id)
      }
    }

    @inline final def apply(start: Option[Long], end: Option[Long], excludeStart: Boolean, excludeEnd: Boolean, forward: Boolean)(implicit db: DBSessionQueryable): Cursor[V] =
      new CursorImpl[Long, V, V](this.getMeta.id, start, end, excludeStart, excludeEnd, if (forward) 1 else -1, v => v)


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

    def defUniqueIndex[K: Packer](name: String, keyGetter: V => K): UniqueIndex[K, V] = {
      this.indexes.synchronized[UniqueIndex[K, V]] {
        if (this.indexes.get(name).isDefined) {
          throw new IllegalArgumentException( s"""index naming "$name" exist!""")
        }
        val idx = new UniqueIndexImpl[K](name, keyGetter)
        this.indexes.put(name, idx)
        idx
      }
    }

    def defPartitionIndex[P: Packer, K: Packer](p: P, name: String, keyGetter: V => K): UniqueIndex[K, V] = {
      this.indexes.synchronized[UniqueIndex[K, V]] {
        val rawIndex = this.indexes.getOrElseUpdate(name, new UniqueIndexImpl[(P, K)](name, v => (p, keyGetter(v)))).asInstanceOf[UniqueIndexImpl[(P, K)]]
        rawIndex.subIndex[P, K](p, (p, k) => (p, k), _._2)
      }
    }
  }

  trait IndexEntryHelper[KB]

  trait IndexEntryInserter[KB] extends IndexEntryHelper[KB] {
    def onKeyEntityInsert(newEntity: KB)(implicit db: DBSessionUpdatable)
  }

  trait IndexEntryUpdater[KB] extends IndexEntryHelper[KB] {
    def onKeyEntityUpdate(oldEntity: KB, newEntity: KB)(implicit db: DBSessionUpdatable)
  }

  trait IndexEntryDeleter[KB] extends IndexEntryHelper[KB] {
    def onKeyEntityDelete(oldEntity: KB)(implicit db: DBSessionUpdatable)
  }

  object cidPrefixHelper {
    val cidPrefixPacker = Packer.tuple1[Int]
    val cidPrefixLen = cidPrefixPacker.packLength(Tuple1(0))

    @inline final def cidPrefix(cid: Int, bigger: Boolean = false) = {
      val buf = new Array[Byte](if (bigger) cidPrefixLen + 1 else cidPrefixLen)
      cidPrefixPacker.pack(Tuple1(cid), buf, 0)
      if (bigger) buf(cidPrefixLen) = 0xFF.asInstanceOf[Byte]
      buf
    }
  }


  class CursorImpl[K: Packer, V: Packer, T <: Entity](cid: Int, keyStart: Option[K], keyEnd: Option[K],
                                                      excludeStart: Boolean, excludeEnd: Boolean, step: Int, v2t: V => T)
                                                     (implicit db: DBSessionQueryable) extends Cursor[T] {
    self =>
    var remaining: Int = Int.MaxValue
    val fullKeyPacker = implicitly[Packer[(Int, K)]]

    def packFullKey(key: Option[K], end: Boolean) = {
      key.fold[Array[Byte]](cidPrefixHelper.cidPrefix(cid, end))(k => this.fullKeyPacker((cid, k)))
    }

    val rangeStart: Array[Byte] = packFullKey(keyStart, false)
    val rangeEnd: Array[Byte] = packFullKey(keyEnd, true)
    val dbCursor: DBCursor = db.newCursor

    def checkRange() = {
      if (remaining > 0) {
        if (step > 0) {
          val c = Util.compareUInt8s(dbCursor.key(), rangeEnd)
          if (c > 0 || (c == 0 && excludeEnd)) {
            remaining = 0
          }
        } else {
          val c = Util.compareUInt8s(dbCursor.key(), rangeStart)
          if (c < 0 || (c == 0 && excludeStart)) {
            remaining = 0
          }
        }
      }
    }

    if (step match {
      case 1 =>
        dbCursor.seek(rangeStart)
      case -1 =>
        dbCursor.seek(rangeEnd)
    }) {
      checkRange()
    } else {
      remaining = 0
    }
    if (remaining <= 0) {
      this.close()
    }

    override def size: Int = 0

    override def slice(from: Int, until: Int): Cursor[T] = {
      if (from > 0) {
        var toDrop = from
        do {
          val skip = toDrop max 100
          dbCursor.next(step * skip)
          remaining -= skip
          checkRange()
          toDrop -= skip
        } while (toDrop > 0 && remaining > 0)
      }
      remaining = remaining min (until - from)
      if (remaining <= 0) {
        close()
      }
      this
    }

    def next(): T = {
      if (remaining <= 0) {
        throw new IllegalStateException("EOF")
      }
      val value = implicitly[Packer[V]].unapply(this.dbCursor.value())
      checkRange()
      remaining -= 1
      if (remaining <= 0) {
        this.close()
      }
      v2t(value)
    }

    def hasNext = remaining > 0

    def close() = this.dbCursor.close()
  }

}
