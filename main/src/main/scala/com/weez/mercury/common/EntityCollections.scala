package com.weez.mercury.common

import scala.reflect.runtime.universe.TypeTag

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

  def forPartitionHost[V <: Entity : Packer : TypeTag](pc: SubCollection[V]): SubHostCollectionImpl[V] = {
    this.hosts.synchronized {
      this.hosts.get(pc.name) match {
        case Some(host: SubHostCollectionImpl[V]) => host
        case None =>
          val typeTag = implicitly[TypeTag[V]]
          val host = new SubHostCollectionImpl[V](pc.name)
          this.hosts.put(pc.name, host)
          this.hostsByType.put(typeTag, host)
          host
        case _ =>
          throw new IllegalArgumentException( s"""PartitionCollection name conflict :${pc.name}""")
      }
    }
  }

  def newHost[V <: Entity : Packer : TypeTag](name: String): HostCollectionImpl[V] = {
    this.hosts.synchronized {
      if (this.hosts.contains(name)) {
        throw new IllegalArgumentException( s"""HostCollection naming "$name" exist!""")
      }
      val typeTag = implicitly[TypeTag[V]]
      if (this.hostsByType.contains(typeTag)) {
        throw new IllegalArgumentException( s"""HostCollection typed "$typeTag" exist!""")
      }
      val host = new HostCollectionImpl[V](name, typeTag) {}
      this.hosts.put(name, host)
      this.hostsByType.put(typeTag, host)
      host
    }
  }

  val hosts = collection.mutable.HashMap[String, HostCollectionImpl[_]]()
  val hostsByID = collection.mutable.HashMap[Int, HostCollectionImpl[_]]()
  val hostsByType = collection.mutable.HashMap[TypeTag[_], HostCollectionImpl[_]]()

  abstract class HostCollectionImpl[V <: Entity : Packer](val name: String, val typeTag: TypeTag[V]) {
    host =>
    @volatile var _meta: DBType.CollectionMeta = null

    def bindDB()(implicit db: DBSessionQueryable): Unit = {
      if (synchronized {
        if (_meta == null) {
          _meta = db.getRootCollectionMeta(this.name)
          if (_meta == null) {
            throw new IllegalArgumentException( s"""no such HostCollection named ${this.name}""")
          }
          indexes.synchronized {
            for (idx <- indexes.values) {
              idx.indexID = _meta.indexPrefixOf(idx.name)
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
      meta.indexPrefixOf(name)
    }

    abstract class IndexBaseImpl[K: Packer](keyGetter: V => K)
      extends UniqueIndex[K, V]
      with EntityCollectionDeleteListener[V]
      with EntityCollectionUpdateListener[V]
      with EntityCollectionInsertListener[V] {
      index =>

      host.addListener(this)

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

      @inline final def getFullKeyOfValue(value: V)(implicit db: DBSessionQueryable): (Int, K) = getFullKey(keyGetter(value))

      @inline final def getRefID(key: K)(implicit db: DBSessionQueryable): Option[Long] = db.get(getFullKey(key))(fullKeyPacker, implicitly[Packer[Long]])

      @inline final override def apply(key: K)(implicit db: DBSessionQueryable): Option[V] = {
        this.getRefID(key).flatMap(db.get[Long, V])
      }

      @inline final override def delete(key: K)(implicit db: DBSessionUpdatable): Unit = {
        this.getRefID(key).foreach(host.delete)
      }

      @inline final override def apply(start: K, end: K)(implicit db: DBSessionQueryable): Cursor[V] =
        this.apply(Range.BoundaryRange(Range.Include(start), Range.Include(end)))

      @inline final override def apply(range: Range[K], forward: Boolean = true)(implicit db: DBSessionQueryable): Cursor[V] = {
        this.newCursor(range.map(r => (this.getIndexID, r)), forward)
      }

      @inline final def newCursor(range: DBRange, forward: Boolean = true)(implicit db: DBSessionQueryable): Cursor[V] = {
        new CursorImpl[K, Long, V](range, forward) {
          override def v2r(id: Long): V = db.get[Long, V](id).get
        }
      }

      @inline final override def update(value: V)(implicit db: DBSessionUpdatable): Unit = {
        host.update(value)
      }

      def onEntityInsert(newEntity: V)(implicit db: DBSessionUpdatable): Unit = {
        db.put(this.getFullKeyOfValue(newEntity), newEntity.id)(this.fullKeyPacker, implicitly[Packer[Long]])
      }

      def onEntityDelete(oldEntity: V)(implicit db: DBSessionUpdatable): Unit = {
        db.del(this.getFullKeyOfValue(oldEntity))(this.fullKeyPacker)
      }

      def onEntityUpdate(oldEntity: V, newEntity: V)(implicit db: DBSessionUpdatable): Unit = {
        val oldIndexEntryKey = keyGetter(oldEntity)
        val newIndexEntryKey = keyGetter(newEntity)
        if (oldIndexEntryKey != newIndexEntryKey) {
          db.del(this.getFullKey(oldIndexEntryKey))
          db.put(this.getFullKey(newIndexEntryKey), newEntity.id)
        }
      }

    }

    val indexes = collection.mutable.Map[String, IndexBaseImpl[_]]()


    @inline final def addListener(listener: EntityCollectionListener[V]) =
      this.regListener(listener, None, reg = true)

    @inline final def removeListener(listener: EntityCollectionListener[V]) =
      this.regListener(listener, None, reg = false)

    def regListener[VL <: Entity](listenerKey: EntityCollectionListener[VL], v2vl: Option[V => VL], reg: Boolean): Unit = {

    }

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

    @inline final def newID()(implicit db: DBSessionUpdatable) = this.fixID(db.newEntityId())


    @inline final def apply(id: Long)(implicit db: DBSessionQueryable): Option[V] = {
      if (id == 0) {
        None
      } else if (collectionIDOf(id) != this.meta.id) {
        throw new IllegalArgumentException("not in this collection")
      } else {
        db.get[Long, V](id)
      }
    }

    final def apply(range: Range[Long], forward: Boolean)(implicit db: DBSessionQueryable): Cursor[V] = {
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

    def defUniqueIndex[K: Packer : TypeTag](indexName: String, keyGetter: V => K): UniqueIndex[K, V] = {
      this.indexes.synchronized[UniqueIndex[K, V]] {
        if (this.indexes.get(indexName).isDefined) {
          throw new IllegalArgumentException( s"""index naming "$indexName" exist!""")
        }
        val idx = new IndexBaseImpl[K](keyGetter) {
          val name = indexName
        }
        this.indexes.put(name, idx)
        idx
      }
    }

    final def defIndex[K: Packer : TypeTag](name: String, getKey: V => K): Index[K, V] = {
      ???
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


  abstract class CursorImpl[K: Packer, V: Packer, R <: Entity](range: DBRange, forward: Boolean)(implicit db: DBSessionQueryable) extends Cursor[R] {
    self =>
    def v2r(v: V): R

    val step = if (forward) 1 else -1
    val rangeStart = range.keyStart
    val rangeEnd = range.keyEnd
    var dbCursor: DBCursor = null
    var remaining = 0
    if (Range.ByteArrayOrdering.compare(rangeStart, rangeEnd) < 0) {
      dbCursor = db.newCursor()
      remaining = Int.MaxValue
      checkRemain(dbCursor.seek(if (forward) rangeStart else rangeEnd), 0)
    }

    def checkRemain(valid: Boolean, count: Int = 1): Unit = {
      if (valid) {
        if (forward) {
          if (Range.ByteArrayOrdering.compare(dbCursor.key(), rangeEnd) >= 0) {
            close()
          } else {
            remaining -= count
            if (remaining <= 0) {
              close()
            }
          }
        } else {
          //backward
          val key = dbCursor.key()
          if (Range.ByteArrayOrdering.compare(key, rangeStart) < 0) {
            close()
          } else if (count == 0) {
            //seek
            if (Range.ByteArrayOrdering.compare(key, rangeEnd) >= 0) {
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

  @packable
  case class SCE[V <: Entity](ownerID: Long, v: V) extends Entity {
    def id = v.id
  }

  import scala.reflect.runtime.universe._

  class SubHostCollectionImpl[V <: Entity : Packer : TypeTag](name: String) extends HostCollectionImpl[SCE[V]](name, typeTag[SCE[V]]) {
    subHost =>
    final def update(ownerID: Long, value: V)(implicit db: DBSessionUpdatable): Unit = {
      super.update(SCE(ownerID, value))
    }

    def defUniqueIndex[K: Packer : TypeTag](ownerID: Long, indexName: String, keyGetter: V => K): UniqueIndex[K, V] = {
      indexes.synchronized[UniqueIndex[K, V]] {
        val rawIndex = this.indexes.getOrElseUpdate(indexName,
          new IndexBaseImpl[(Long, K)](v => (ownerID, keyGetter(v.v))) {

            override val name = indexName
          }
        ).asInstanceOf[IndexBaseImpl[(Long, K)]]
        new UniqueIndex[K, V] {

          override def update(value: V)(implicit db: DBSessionUpdatable): Unit = rawIndex.update(SCE(value.id, value))

          override def delete(key: K)(implicit db: DBSessionUpdatable): Unit = rawIndex.delete((ownerID, key))

          override def apply(key: K)(implicit db: DBSessionQueryable): Option[V] = rawIndex.apply((ownerID, key)).map(_.v)

          def apply(start: K, end: K)(implicit db: DBSessionQueryable): Cursor[V] = {
            this.apply(Range.BoundaryRange(Range.Include(start), Range.Include(end)))
          }

          override def apply(range: Range[K], forward: Boolean)(implicit db: DBSessionQueryable): Cursor[V] = {
            new CursorImpl[K, Long, V](range.map(r => (rawIndex.getIndexID, ownerID, r)), forward) {
              override def v2r(id: Long): V = db.get[Long, SCE[V]](id).get.v
            }
          }
        }
      }
    }

    val sce2v = Some((s: SCE[V]) => s.v)

    @inline final def addSubListener(listener: EntityCollectionListener[V]) = {
      this.regListener(listener, sce2v, reg = true)
    }

    @inline final def removeSubListener(listener: EntityCollectionListener[V]) = {
      this.regListener(listener, sce2v, reg = false)
    }

    final def defIndex[K: Packer : TypeTag](ownerID: Long, name: String, getKey: V => K): Index[K, V] = {
      ???
    }
  }

}


abstract class DataView[K, V] {

  def apply(key: K): Option[V] = {
    ???
  }

  def name: String

  protected trait Tracer[BUF, E <: Entity] {
    def subRefs = Seq.empty[SubRef[_]]

    def extract(entity: E, buf: BUF)

    def isChanged(oldB: BUF, newB: BUF): Boolean

    protected abstract class SubRef[R <: Entity](refIndex: IndexBase[_ >: Ref[R]
      with Product1[Ref[R]]
      with Product2[Ref[R], Nothing]
      with Product3[Ref[R], Nothing, Nothing]
      with Product4[Ref[R], Nothing, Nothing, Nothing]
      with Product5[Ref[R], Nothing, Nothing, Nothing, Nothing]
      with Product6[Ref[R], Nothing, Nothing, Nothing, Nothing, Nothing]
      with Product7[Ref[R], Nothing, Nothing, Nothing, Nothing, Nothing, Nothing]
      , E]) extends Tracer[BUF, R] with EntityCollectionListener[R] {

    }

  }


  protected abstract class DataViewMeta[BUF, ROOT <: Entity : TypeTag] extends Tracer[BUF, ROOT] {

    def extractKey(buf: BUF): Map[K, V]
  }

  class MyBuffer {
    var i1: Int = _
    var i2: Int = _
  }

  protected def meta(): DataViewMeta[_, _]

  private lazy val preparedMeta = {
    val meta = this.meta()
  }
}
