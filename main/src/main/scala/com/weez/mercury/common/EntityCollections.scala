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

  def forPartitionHost[V <: Entity : Packer](pc: SubCollection[V]): SubHostCollectionImpl[V] = {
    this.hosts.synchronized {
      this.hosts.get(pc.name) match {
        case Some(host: SubHostCollectionImpl[V]) => host
        case None =>
          val host = new SubHostCollectionImpl[V](pc.name)
          this.hosts.put(pc.name, host)
          host
        case _ =>
          throw new IllegalArgumentException( s"""PartitionCollection name conflict :${pc.name}""")
      }
    }
  }

  def newHost[V <: Entity : Packer](name: String): HostCollectionImpl[V] = {
    this.hosts.synchronized {
      if (this.hosts.get(name).isDefined) {
        throw new IllegalArgumentException( s"""HostCollection naming"$name" exist!""")
      }
      val host = new HostCollectionImpl[V](name) {
        override val valuePacker: Packer[V] = implicitly[Packer[V]]
      }
      this.hosts.put(name, host)
      host
    }
  }

  val hosts = collection.mutable.HashMap[String, HostCollectionImpl[_]]()
  val hostsByID = collection.mutable.HashMap[Int, HostCollectionImpl[_]]()

  abstract class HostCollectionImpl[V <: Entity](val name: String) {
    host =>
    @volatile var _meta: RootCollectionMeta = null
    implicit val valuePacker: Packer[V]

    def bindDB()(implicit db: DBSessionQueryable): Unit = {
      if (synchronized {
        if (_meta == null) {
          _meta = db.schema.getRootCollectionMeta(this.name)
          if (_meta == null) {
            throw new IllegalArgumentException( s"""no such HostCollection named ${this.name}""")
          }
          indexes.synchronized {
            for (idx <- indexes.values) {
              idx.indexID = _meta.indexes.find(i => i.name == idx.name).get.prefix
            }
          }
          true
        } else {
          false
        }
      }) {
        hostsByID.synchronized {
          hostsByID.put(_meta.prefix, this)
        }
      }
    }

    @inline final def meta(implicit db: DBSessionQueryable) = {
      if (_meta == null) {
        bindDB()
      }
      _meta
    }

    @inline final def getIndexID(name: String)(implicit db: DBSessionQueryable) = {
      meta.indexes.find(i => i.name == name).get.prefix
    }

    abstract class IndexBaseImpl[K: Packer, KB <: Entity, R <: Entity : Packer](keyGetter: KB => K)
      extends UniqueIndex[K, R]
      with IndexEntryInserter[KB]
      with IndexEntryDeleter[KB]
      with IndexEntryUpdater[KB] {
      index =>

      def kb2r(keyBase: KB): R

      def r2v(r: R): V

      def v2r(v: V): R

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

      @inline final def getFullKeyOfKB(keyBase: KB)(implicit db: DBSessionQueryable): (Int, K) = getFullKey(keyGetter(keyBase))

      @inline final def getRefID(key: K)(implicit db: DBSessionQueryable): Option[Long] = db.get(getFullKey(key))(fullKeyPacker, implicitly[Packer[Long]])

      @inline final override def apply(key: K)(implicit db: DBSessionQueryable): Option[R] = {
        this.getRefID(key).flatMap(db.get[Long, V]).map(v2r)
      }

      @inline final override def delete(key: K)(implicit db: DBSessionUpdatable): Unit = {
        this.getRefID(key).foreach(host.delete)
      }

      @inline final override def apply(start: Option[K], end: Option[K], excludeStart: Boolean, excludeEnd: Boolean, forward: Boolean)(implicit db: DBSessionQueryable): Cursor[R] = {
        val range = new ScanRange[K](start, end, excludeStart, excludeEnd) {
          val prefixPacker = implicitly[Packer[Tuple1[Int]]]

          override def buildPrefixMin = prefixPacker(Tuple1(index.getIndexID))

          override def buildFullKey(key: K) = fullKeyPacker((index.getIndexID, key))

        }
        new CursorImpl[K, Long, R](range, forward) {
          override def v2r(id: Long) = index.v2r(db.get[Long, V](id).get)
        }
      }


      override def subIndex[PK: Packer, SK](keyMapper: SubKeyMapper[K, PK, SK]) = new UniqueIndex[SK, R] {
        val partKeyPacker = implicitly[Packer[(Int, PK)]]

        override def update(value: R)(implicit db: DBSessionUpdatable): Unit = index.update(value)

        override def delete(key: SK)(implicit db: DBSessionUpdatable): Unit = index.delete(keyMapper.fullKey(key))

        override def apply(key: SK)(implicit db: DBSessionQueryable): Option[R] = index(keyMapper.fullKey(key))

        override def apply(start: Option[SK], end: Option[SK], excludeStart: Boolean, excludeEnd: Boolean, forward: Boolean)(implicit db: DBSessionQueryable): Cursor[R] = {
          val range = new ScanRange[K](start.map(keyMapper.fullKey), end.map(keyMapper.fullKey), excludeStart, excludeEnd) {

            override def buildPrefixMin = partKeyPacker((index.getIndexID, keyMapper.prefixKey))

            override def buildFullKey(key: K) = fullKeyPacker((index.getIndexID, key))

          }
          new CursorImpl[K, Long, R](range, forward) {
            override def v2r(id: Long): R = db.get[Long, R](id).get
          }
        }
      }

      @inline final override def update(r: R)(implicit db: DBSessionUpdatable): Unit = {
        host.update(r2v(r))
      }

      def onKeyEntityInsert(newEntity: KB)(implicit db: DBSessionUpdatable): Unit = {
        db.put(this.getFullKeyOfKB(newEntity), newEntity.id)(this.fullKeyPacker, implicitly[Packer[Long]])
      }

      def onKeyEntityDelete(oldEntity: KB)(implicit db: DBSessionUpdatable): Unit = {
        db.del(this.getFullKeyOfKB(oldEntity))(this.fullKeyPacker)
      }

      def onKeyEntityUpdate(oldEntity: KB, newEntity: KB)(implicit db: DBSessionUpdatable): Unit = {
        val oldIndexEntryKey = keyGetter(oldEntity)
        val newIndexEntryKey = keyGetter(newEntity)
        if (!oldIndexEntryKey.equals(newIndexEntryKey)) {
          db.del(this.getFullKey(oldIndexEntryKey))
          db.put(this.getFullKey(newIndexEntryKey), newEntity.id)
        }
      }

    }

    val indexes = collection.mutable.Map[String, IndexBaseImpl[_, _, _]]()

    /**
     * 内部索引和外部的Extend索引
     */
    private var idxUpdaters = Seq[IndexEntryUpdater[V]]()
    private var idxInserters = Seq[IndexEntryInserter[V]]()
    private var idxDeleters = Seq[IndexEntryDeleter[V]]()

    def registerIndexEntryHelper(er: AnyRef): Unit = {
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

    @inline final def fixID(id: Long)(implicit db: DBSessionQueryable) = entityIDOf(this.meta.prefix, id)

    @inline final def fixIDAndGet(id: Long)(implicit db: DBSessionQueryable): Option[V] = db.get(this.fixID(id))

    @inline final def newID()(implicit db: DBSessionUpdatable) = this.fixID(db.schema.newEntityID())


    @inline final def apply(id: Long)(implicit db: DBSessionQueryable): Option[V] = {
      if (id == 0) {
        None
      } else if (collectionIDOf(id) != this.meta.id) {
        throw new IllegalArgumentException("not in this collection")
      } else {
        db.get[Long, V](id)
      }
    }

    final def apply(start: Option[Long], end: Option[Long], excludeStart: Boolean, excludeEnd: Boolean, forward: Boolean)(implicit db: DBSessionQueryable): Cursor[V] = {
      val range = new ScanRange(start, end, excludeStart, excludeEnd) {
        override def buildPrefixMin = Packer.LongPacker(fixID(0l))

        override def buildPrefixMax = Packer.LongPacker(fixID(-1l))

        override def buildFullKey(id: Long) = Packer.LongPacker(fixID(id))
      }
      new CursorImpl[Long, V, V](range, forward) {
        override def v2r(v: V): V = v
      }
    }

    @inline final def checkID(id: Long)(implicit db: DBSessionUpdatable): Long = {
      if (collectionIDOf(id) != this.meta.id) {
        throw new IllegalArgumentException("id is not belong this collection")
      }
      id
    }

    final def update(value: V)(implicit db: DBSessionUpdatable): Unit = {
      val id = checkID(value.id)
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
      checkID(id)
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

    def defUniqueIndex[K: Packer](indexName: String, keyGetter: V => K): UniqueIndex[K, V] = {
      this.indexes.synchronized[UniqueIndex[K, V]] {
        if (this.indexes.get(indexName).isDefined) {
          throw new IllegalArgumentException( s"""index naming "$indexName" exist!""")
        }
        val idx = new IndexBaseImpl[K, V, V](keyGetter) {
          val name = indexName

          override def v2r(v: V) = v

          override def r2v(r: V) = r

          override def kb2r(keyBase: V) = keyBase
        }
        this.indexes.put(name, idx)
        idx
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

  val cidPrefixPacker = Packer.tuple1[Int]

  final val b_one = 1.asInstanceOf[Byte]
  final val b_ff = 0xff.asInstanceOf[Byte]
  final val b_zero = 0.asInstanceOf[Byte]

  abstract class ScanRange[K](keyStart: Option[K], keyEnd: Option[K], excludeStart: Boolean, excludeEnd: Boolean) {
    private final def keyAdd(key: Array[Byte], v: Int) = {
      def add(i: Int, v: Int): Unit = {
        if (i < 0 || v == 0) {
          return
        }
        val r = (key(i) & 0xFF) + v
        key(i) = r.asInstanceOf[Byte]
        add(i - 1, r >> 8)
      }
      add(key.length - 1, v)
      key
    }

    def buildPrefixMin: Array[Byte]

    def buildPrefixMax = {
      val k = buildPrefixMin
      k(k.length - 1) = -1
      k
    }

    def buildFullKey(key: K): Array[Byte]

    @inline final def rangeMin(): Array[Byte] = {
      keyAdd(keyStart.fold(buildPrefixMin)(buildFullKey), if (excludeStart) 1 else 0)
    }

    @inline final def rangeMax(): Array[Byte] = {
      keyAdd(keyEnd.fold(buildPrefixMin)(buildFullKey), if (excludeEnd) -1 else 0)
    }
  }


  abstract class CursorImpl[K: Packer, V: Packer, R <: Entity](range: ScanRange[K], forward: Boolean)(implicit db: DBSessionQueryable) extends Cursor[R] {
    self =>
    def v2r(v: V): R

    val step = if (forward) 1 else -1
    val rangeMin = range.rangeMin()
    val rangeMax = range.rangeMax()
    var dbCursor: DBCursor = null
    var remaining = 0
    if (Util.compareUInt8s(rangeMin, rangeMax) > 0) {
      dbCursor = db.newCursor()
      remaining = Int.MaxValue
      checkRemain(dbCursor.seek(if (forward) rangeMin else rangeMax), 0)
    }

    def checkRemain(valid: Boolean, count: Int = 1): Unit = {
      if (valid) {
        if (forward) {
          if (Util.compareUInt8s(dbCursor.key(), rangeMax) > 0) {
            close()
          } else {
            remaining -= count
            if (remaining <= 0) {
              close()
            }
          }
        } else {
          val key = dbCursor.key()
          if (Util.compareUInt8s(key, rangeMin) < 0) {
            close()
          } else if (count == 0) {
            //seek
            if (Util.compareUInt8s(key, rangeMax) > 0) {
              checkRemain(dbCursor.next(step))
            }
          } else {
            remaining -= count
            if (remaining <= 0) {
              close()
            }
          }
        }
      } else {
        close()
      }
    }

    override def size: Int = {
      var s = 0
      if (hasNext) {
        val step = if (forward) 1 else -1
        do {
          s += 1
          checkRemain(dbCursor.next(step))
        } while (!hasNext)
      }
      s
    }

    override def slice(from: Int, until: Int): Cursor[R] = {
      if (hasNext) {
        val r = until - from
        if (r <= 0 || remaining < from) {
          close()
        } else if (from > 0) {
          var toDrop = from
          do {
            val skip = toDrop max 100
            checkRemain(dbCursor.next(step * skip), skip)
            toDrop -= skip
          } while (toDrop > 0 && hasNext)
        }
        remaining = remaining min r
      }
      this
    }

    override def next(): R = {
      if (!hasNext) {
        throw new IllegalStateException("EOF")
      }
      val value = implicitly[Packer[V]].unapply(dbCursor.value())
      checkRemain(dbCursor.next(step))
      v2r(value)
    }

    @inline final def hasNext = remaining > 0

    @inline override final def close() = if (dbCursor != null) {
      dbCursor.close()
      dbCursor = null
      remaining = 0
    }
  }

  case class SCE[V <: Entity](ownerID: Long, v: V) extends Entity {
    def id = v.id
  }

  class SubHostCollectionImpl[V <: Entity : Packer](name: String) extends HostCollectionImpl[SCE[V]](name) {
    override implicit val valuePacker: Packer[SCE[V]] = Packer(SCE[V] _)

    final def update(ownerID: Long, value: V)(implicit db: DBSessionUpdatable): Unit = {
      super.update(SCE(ownerID, value))
    }

    def defUniqueIndex[K: Packer](ownerID: Long, indexName: String, keyGetter: V => K): UniqueIndex[K, V] = {
      indexes.synchronized[UniqueIndex[K, V]] {
        val rawIndex = this.indexes.getOrElseUpdate(indexName,
          new IndexBaseImpl[(Long, K), SCE[V], V](v => (ownerID, keyGetter(v.v))) {
            override val name = indexName

            override def kb2r(keyBase: SCE[V]): V = keyBase.v

            override def v2r(v: SCE[V]): V = v.v

            override def r2v(r: V): SCE[V] = SCE(ownerID, r)

          }
        ).asInstanceOf[IndexBaseImpl[(Long, K), SCE[V], V]]
        rawIndex.subIndex[Long, K](new SubKeyMapper[(Long, K), Long, K] {

          override def prefixKey: Long = ownerID

          override def fullKey(subKey: K) = (ownerID, subKey)

          override def subKey(fullKey: (Long, K)): K = fullKey._2
        })
      }
    }

  }

}
