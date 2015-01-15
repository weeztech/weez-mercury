package com.weez.mercury.common


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
    var collectionID: Int = 0
    val valuePacker: Packer[V] = implicitly[Packer[V]]

    def bindDB()(implicit db: DBSessionQueryable): Unit = {
      if (this.synchronized {
        if (this.collectionID == 0) {
          val meta = db.schema.getHostCollection(this.name)
          this.collectionID = meta.id
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
          hostsByID.put(this.collectionID, this)
        }
      }
    }


    @inline final def cid(implicit db: DBSessionQueryable) = {
      if (this.collectionID == 0) {
        this.bindDB()
      }
      this.collectionID
    }

    abstract class IndexBaseImpl[K: Packer, KB <: Entity](keyGetter: KB => K) extends UniqueIndex[K, V] {

      registerIndexEntryHelper(this)

      val name: String
      var indexID = 0

      def getIndexID(implicit db: DBSessionQueryable) = {
        if (this.indexID == 0) {
          host.bindDB()
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

    @inline private[common] final def fixID(id: Long)(implicit db: DBSessionQueryable) = entityIDOf(this.cid, id)

    @inline private[common] final def fixIDAndGet(id: Long)(implicit db: DBSessionQueryable): Option[V] = db.get(this.fixID(id))

    @inline final def newID()(implicit db: DBSessionUpdatable) = this.fixID(db.schema.newEntityID())


    @inline final def apply(id: Long)(implicit db: DBSessionQueryable): Option[V] = {
      if (id == 0) {
        None
      } else if (collectionIDOf(id) != this.cid) {
        throw new IllegalArgumentException("not in this collection")
      } else {
        db.get[Long, V](id)
      }
    }

    @inline final def apply(start: Option[Long], end: Option[Long], excludeStart: Boolean, excludeEnd: Boolean)(implicit db: DBSessionQueryable): Cursor[V] =
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

    def defUniqueIndex[K: Packer](name: String, keyGetter: V => K): UniqueIndex[K, V] = {
      this.indexes.synchronized[UniqueIndex[K, V]] {
        if (this.indexes.get(name).isDefined) {
          throw new IllegalArgumentException( s"""index naming "$name" exist!""")
        }
        this.indexes.put(name, new UniqueIndexImpl[K](name, keyGetter)).asInstanceOf[UniqueIndex[K, V]]
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

  class CursorImpl[K: Packer, V: Packer, T <: Entity](kCID: Int, keyStart: Option[K], keyEnd: Option[K],
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

}
