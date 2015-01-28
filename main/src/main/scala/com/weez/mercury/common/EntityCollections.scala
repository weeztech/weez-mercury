package com.weez.mercury.common


import scala.reflect.runtime.universe._

private object EntityCollections {

  @inline final def collectionIDOf(entityID: Long): Int = (entityID >>> 48).asInstanceOf[Int]

  @inline final def entityIDOf(collectionID: Int, rawID: Long): Long = (rawID & 0xFFFFFFFFFFFFL) | (collectionID.asInstanceOf[Long] << 48)

  @inline final def getEntity[V <: Entity](id: Long)(implicit db: DBSessionQueryable): V = {
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

  def forPartitionHost[O <: Entity, V <: Entity : Packer](pc: SubCollection[O, V]): SubHostCollectionImpl[O, V] = {
    this.hosts.synchronized {
      this.hosts.get(pc.name) match {
        case Some(host: SubHostCollectionImpl[O, V]) => host
        case None =>
          val host = new SubHostCollectionImpl[O, V](pc.name)
          this.hosts.put(pc.name, host)
          host
        case _ =>
          throw new IllegalArgumentException( s"""PartitionCollection name conflict :${pc.name}""")
      }
    }
  }

  def newHost[V <: Entity : Packer](name: String): HostCollectionImpl[V] = {
    this.hosts.synchronized {
      if (this.hosts.contains(name)) {
        throw new IllegalArgumentException( s"""HostCollection naming "$name" exist!""")
      }
      val host = new HostCollectionImpl[V](name)
      this.hosts.put(name, host)
      host
    }
  }

  def newDataView[K: Packer, V: Packer](name: String): DataViewImpl[K, V] = {
    this.dataViews.synchronized {
      if (this.dataViews.contains(name)) {
        throw new IllegalArgumentException( s"""DataView naming "$name" exist!""")
      }
      val view = new DataViewImpl[K, V](name)
      this.dataViews.put(name, view)
      view
    }
  }

  val dataViews = collection.mutable.HashMap[String, DataViewImpl[_, _]]()

  val hosts = collection.mutable.HashMap[String, HostCollectionImpl[_]]()
  val hostsByID = collection.mutable.HashMap[Int, HostCollectionImpl[_]]()

  final val RefType = typeOf[Ref[Entity]]

  final val ProductType = typeOf[Product]

  class HostCollectionImpl[V <: Entity : Packer](val name: String) {
    host =>
    val valuePacker = implicitly[Packer[V]]

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

    abstract class IndexBaseImpl[FK](val name: String)(implicit fullKeyPacker: Packer[FK])
      extends EntityCollectionListener[V] {
      index =>

      def hostCollection = host

      host.addListener(this)

      var indexID = 0

      def getIndexID(implicit db: DBSessionQueryable) = {
        if (this.indexID == 0) {
          this.indexID = host.getIndexID(this.name)
        }
        this.indexID
      }

      def v2fk(value: V)(implicit db: DBSessionQueryable): FK

      @inline final def deleteByFullKey(fullKey: FK)(implicit db: DBSessionUpdatable): Unit = {
        val id = db.get[FK, Long](fullKey)
        if (id.isDefined) {
          host.delete(id.get)
        }
      }

      @inline final def getByFullKey(fullKey: FK)(implicit db: DBSessionQueryable): Option[V] = {
        val id = db.get[FK, Long](fullKey)
        if (id.isDefined) db.get[Long, V](id.get) else None
      }

      def onEntityInsert(newEntity: V)(implicit db: DBSessionUpdatable): Unit = {
        db.put(v2fk(newEntity), newEntity.id)
      }

      def onEntityDelete(oldEntity: V)(implicit db: DBSessionUpdatable): Unit = {
        db.del(v2fk(oldEntity))
      }

      def onEntityUpdate(oldEntity: V, newEntity: V)(implicit db: DBSessionUpdatable): Unit = {
        val oldIndexEntryKey = v2fk(oldEntity)
        val newIndexEntryKey = v2fk(newEntity)
        if (oldIndexEntryKey != newIndexEntryKey) {
          db.del(oldIndexEntryKey)
          db.put(newIndexEntryKey, newEntity.id)
        }
      }

    }

    private case class ListenerInfo(l: EntityCollectionListener[V], var refCount: Int)

    private val listeners = scala.collection.mutable.AnyRefMap[EntityCollectionListener[_], ListenerInfo]()
    @volatile private var deleteListeners: Seq[EntityCollectionListener[V]] = Seq.empty
    @volatile private var updateListeners: Seq[EntityCollectionListener[V]] = Seq.empty
    @volatile private var insertListeners: Seq[EntityCollectionListener[V]] = Seq.empty

    def regListener[VL <: Entity](listener: EntityCollectionListener[VL], v2vl: Option[V => VL], reg: Boolean): Unit = {
      listeners synchronized {
        if (reg) {
          listeners.get(listener) match {
            case Some(x: ListenerInfo) =>
              x.refCount += 1
              return
            case None =>
              listeners.update(listener, ListenerInfo(v2vl.fold(listener.asInstanceOf[EntityCollectionListener[V]])(
                v2vl => new EntityCollectionListener[V] {
                  override val canListenEntityDelete = listener.canListenEntityDelete
                  override val canListenEntityUpdate = listener.canListenEntityUpdate
                  override val canListenEntityInsert = listener.canListenEntityInsert

                  override def onEntityInsert(newEntity: V)(implicit db: DBSessionUpdatable): Unit = {
                    listener.onEntityInsert(v2vl(newEntity))
                  }

                  override def onEntityUpdate(oldEntity: V, newEntity: V)(implicit db: DBSessionUpdatable): Unit = {
                    listener.onEntityUpdate(v2vl(oldEntity), v2vl(newEntity))
                  }

                  override def onEntityDelete(oldEntity: V)(implicit db: DBSessionUpdatable): Unit = {
                    listener.onEntityDelete(v2vl(oldEntity))
                  }
                }
              ), 1))
          }
        } else {
          val li = listeners.get(listener).get
          if (li.refCount == 1) {
            listeners.remove(listener)
          } else {
            li.refCount -= 1
            return
          }
        }
        deleteListeners = listeners.values.filter(l => l.l.canListenEntityDelete).map(l => l.l).toSeq
        updateListeners = listeners.values.filter(l => l.l.canListenEntityUpdate).map(l => l.l).toSeq
        insertListeners = listeners.values.filter(l => l.l.canListenEntityInsert).map(l => l.l).toSeq
      }
    }

    @inline final def addListener(listener: EntityCollectionListener[V]) =
      this.regListener(listener, None, reg = true)

    @inline final def removeListener(listener: EntityCollectionListener[V]) =
      this.regListener(listener, None, reg = false)

    @inline final def fixID(id: Long)(implicit db: DBSessionQueryable) = entityIDOf(this.meta.prefix, id)

    @inline final def fixIDAndGet(id: Long)(implicit db: DBSessionQueryable): Option[V] = db.get(this.fixID(id))

    @inline final def newEntityID()(implicit db: DBSessionUpdatable) = this.fixID(db.newEntityId())

    @inline final def apply(id: Long)(implicit db: DBSessionQueryable): Option[V] = {
      if (id == 0) {
        None
      } else if (collectionIDOf(id) != this.meta.id) {
        throw new IllegalArgumentException("not in this collection")
      } else {
        val ov = db.get[Long, V](id)
        if (ov.isDefined) {
          ov.get._id = id
        }
        ov
      }
    }

    final def apply(forward: Boolean)(implicit db: DBSessionQueryable): Cursor[V] = {
      import Range._
      val prefix = this.meta.prefix
      Cursor[V](entityIDOf(prefix, 0) +-+ entityIDOf(prefix, -1L), forward)
    }

    @inline final def checkID(id: Long)(implicit db: DBSessionUpdatable): Long = {
      if (collectionIDOf(id) != this.meta.prefix) {
        throw new IllegalArgumentException( s"""id of $id doesn't belong to this collection""")
      }
      id
    }

    final def insert(value: V)(implicit db: DBSessionUpdatable): Long = {
      val id = this.newEntityID()
      value._id = id
      for (l <- this.insertListeners) {
        l.onEntityInsert(value)
      }
      db.put(id, value)
      id
    }

    final def update(value: V)(implicit db: DBSessionUpdatable): Unit = {
      if (value._id == 0l) {
        insert(value)
        return
      }
      val id = checkID(value._id)
      val old: Option[V] = db.get[Long, V](id)
      if (old.isEmpty) {
        throw new IllegalArgumentException( s"""entity who's id is $id doesn't exist""")
      }
      for (l <- this.updateListeners) {
        l.onEntityUpdate(old.get, value)
      }
      db.put(id, value)
    }

    final def delete(id: Long)(implicit db: DBSessionUpdatable): Unit = {
      checkID(id)
      lazy val old: Option[V] = db.get[Long, V](id)
      for (l <- this.deleteListeners) {
        if (old.isDefined) {
          l.onEntityDelete(old.get)
        }
      }
      db.del(id)
    }

    val indexes = collection.mutable.Map[String, IndexBaseImpl[_]]()

    abstract class HostIndexBaseImpl[FK, K](name: String)(implicit fullKeyPacker: Packer[FK], keyPacker: Packer[K], keyTypeTag: TypeTag[K])
      extends IndexBaseImpl[FK](name) {

      def scanReverse[R <: Entity](ref: Ref[R])(implicit db: DBSessionQueryable): Cursor[V] = {
        keyTypeTag match {
          case TypeRef(RefType, _, _) =>
            val start = (this.getIndexID, ref)
            val end = (this.getIndexID, RefSome[R](ref.id + 1))
            Cursor[Long](Range.BoundaryRange(Range.Include(start), Range.Exclude(end)), forward = true).map { id =>
              host(id).get
            }
          case TypeRef(t, _, args) =>
            if (t <:< ProductType && args != Nil && args.head <:< RefType) {
              val start = (this.getIndexID, Tuple1(ref))
              val end = (this.getIndexID, Tuple1(RefSome[R](ref.id + 1)))
              Cursor[Long](Range.BoundaryRange(Range.Include(start), Range.Exclude(end)), forward = true).map { id =>
                host(id).get
              }
            } else {
              throw new UnsupportedOperationException
            }
          case _ =>
            throw new UnsupportedOperationException
        }
      }

      val cursorKeyRangePacker = Packer.of[(Int, RangeBound[K])]

      def newCursor(range: Range[K], forward: Boolean)(implicit db: DBSessionQueryable): Cursor[Long] = {
        val r = range.map(r => (this.getIndexID, r))(cursorKeyRangePacker)
        Cursor[Long](r, forward)
      }
    }

    def defUniqueIndex[K: Packer : TypeTag](indexName: String, keyGetter: V => K): UniqueIndex[K, V] = {
      type FullKey = (Int, K)
      this.indexes.synchronized[UniqueIndex[K, V]] {
        if (this.indexes.get(indexName).isDefined) {
          throw new IllegalArgumentException( s"""index naming "$indexName" exist!""")
        }
        val idx = new HostIndexBaseImpl[FullKey, K](indexName) with UniqueIndex[K, V] {

          override def v2fk(value: V)(implicit db: DBSessionQueryable) = (this.getIndexID, keyGetter(value))

          @inline final override def apply(key: K)(implicit db: DBSessionQueryable): Option[V] = {
            db.get[FullKey, Long](this.getIndexID, key).flatMap(db.get[Long, V])
          }

          @inline final override def delete(key: K)(implicit db: DBSessionUpdatable): Unit = {
            db.get[FullKey, Long](this.getIndexID, key).foreach(host.delete)
          }

          @inline final def update(value: V)(implicit db: DBSessionUpdatable): Unit = {
            host.update(value)
          }

          override def apply(range: Range[K], forward: Boolean)(implicit db: DBSessionQueryable): Cursor[V] = {
            newCursor(range, forward).map(id => host(id).get)
          }
        }
        this.indexes.put(name, idx)
        idx
      }
    }

    final def defIndex[K: Packer : TypeTag](indexName: String, keyGetter: V => K): Index[K, V] = {
      type FullKey = (Int, K, Long)
      this.indexes.synchronized[Index[K, V]] {
        if (this.indexes.get(indexName).isDefined) {
          throw new IllegalArgumentException( s"""index naming "$indexName" exist!""")
        }
        val idx = new HostIndexBaseImpl[FullKey, K](name) with Index[K, V] {
          override def v2fk(value: V)(implicit db: DBSessionQueryable) = (this.getIndexID, keyGetter(value), value.id)

          override def apply(range: Range[K], forward: Boolean)(implicit db: DBSessionQueryable): Cursor[V] = {
            newCursor(range, forward).map(id => host(id).get)
          }
        }
        this.indexes.put(name, idx)
        idx
      }
    }
  }


  @packable
  case class SCE[O <: Entity, V <: Entity](owner: Ref[O], entity: V) extends Entity with SubEntityPair[O, V] {
    this._id = entity.id
  }

  import scala.reflect.runtime.universe._

  class SubHostCollectionImpl[O <: Entity, V <: Entity : Packer](name: String) extends HostCollectionImpl[SCE[O, V]](name) {
    subHost =>
    @inline final def update(owner: Ref[O], value: V)(implicit db: DBSessionUpdatable): Unit = {
      if (value.id == 0) {
        insert(owner, value)
      } else {
        super.update(SCE(owner, value))
      }
    }

    @inline final def insert(owner: Ref[O], value: V)(implicit db: DBSessionUpdatable): Long = {
      val id = super.insert(SCE(owner, value))
      value._id = id
      id
    }


    class SubIndexBaseImpl[FK, K](val rawIndex: SubRawIndexBaseImpl[FK, K], val owner: Ref[O]) {
      def subHostCollection = subHost
    }

    abstract class SubRawIndexBaseImpl[FK: Packer, K: Packer](name: String) extends IndexBaseImpl[FK](name) {
      val cursorKeyRangePacker = Packer.of[(Int, Ref[O], RangeBound[K])]

      def newCursor(owner: Ref[O], range: Range[K], forward: Boolean)(implicit db: DBSessionQueryable): Cursor[Long] = {
        val r = range.map(r => (this.getIndexID, owner, r))(cursorKeyRangePacker)
        Cursor[Long](r, forward)
      }
    }

    def defUniqueIndex[K: Packer : TypeTag](owner: Ref[O], indexName: String, keyGetter: V => K): UniqueIndex[K, V] = {
      type FullKey = (Int, Ref[O], K)
      indexes.synchronized[UniqueIndex[K, V]] {
        val rawIndex = this.indexes.getOrElseUpdate(indexName,
          new SubRawIndexBaseImpl[FullKey, K](indexName) {
            override def v2fk(value: SCE[O, V])(implicit db: DBSessionQueryable) = (this.getIndexID, value.owner, keyGetter(value.entity))
          }
        ).asInstanceOf[SubRawIndexBaseImpl[FullKey, K]]
        new SubIndexBaseImpl[FullKey, K](rawIndex, owner) with UniqueIndex[K, V] {

          override def update(value: V)(implicit db: DBSessionUpdatable): Unit = {
            subHost.update(SCE(owner, value))
          }

          override def delete(key: K)(implicit db: DBSessionUpdatable): Unit = {
            rawIndex.deleteByFullKey((rawIndex.getIndexID, owner, key))
          }

          override def apply(key: K)(implicit db: DBSessionQueryable): Option[V] = {
            rawIndex.getByFullKey(rawIndex.getIndexID, owner, key).map(_.entity)
          }

          override def apply(range: Range[K], forward: Boolean)(implicit db: DBSessionQueryable): Cursor[V] = {
            rawIndex.newCursor(owner, range, forward).map(id => subHost(id).get.entity)
          }
        }
      }
    }

    final def defIndex[K: Packer : TypeTag](owner: Ref[O], indexName: String, keyGetter: V => K): Index[K, V] = {
      type FullKey = (Int, Ref[O], K, Long)
      indexes.synchronized[Index[K, V]] {
        val rawIndex = this.indexes.getOrElseUpdate(indexName,
          new SubRawIndexBaseImpl[FullKey, K](indexName) {
            override def v2fk(value: SCE[O, V])(implicit db: DBSessionQueryable) = (this.getIndexID, value.owner, keyGetter(value.entity), value.id)
          }
        ).asInstanceOf[SubRawIndexBaseImpl[FullKey, K]]
        new SubIndexBaseImpl[FullKey, K](rawIndex, owner) with Index[K, V] {
          override def apply(range: Range[K], forward: Boolean)(implicit db: DBSessionQueryable): Cursor[V] = {
            rawIndex.newCursor(owner, range, forward).map(id => subHost(id).get.entity)
          }
        }
      }
    }
  }

  class DataViewImpl[K, V](name: String)(implicit kPacker: Packer[K], vPacker: Packer[V], fullKeyPacker: Packer[(Int, K)]) {

    def apply(key: K)(implicit db: DBSessionQueryable): Option[V] = db.get[FullKey, V](this.getViewID, key)

    def apply(range: Range[K], forward: Boolean = true)(implicit db: DBSessionQueryable): Cursor[(K, V)] = {
      val r = range.map(r => (getViewID, r))(cursorRangePacker)
      Cursor.raw[V](r, forward).map((k, v) => (fullKeyPacker.unapply(k)._2, v))
    }

    private type FullKey = (Int, K)
    private val cursorRangePacker = Packer.of[(Int, RangeBound[K])]
    private var viewID = 0

    private def getViewID(implicit db: DBSessionQueryable) = {
      if (this.viewID == 0) {
        this.viewID = ???
      }
      this.viewID
    }

    sealed abstract class Tracer[ES <: AnyRef, E <: Entity](val meta: DataView[K, V]#Tracer[ES, E])
      extends EntityCollectionListener[E] {
      tracer =>

      val entitiesCreator: () => ES


      def newSub[S <: Entity](subMeta: DataView[K, V]#Tracer[ES, E]#SubTracer[S]): SubTracer[S] = new SubTracer[S](subMeta)

      def traceUp(entityID: Long, oldBuf: ES, newBuf: ES)(implicit db: DBSessionUpdatable): Unit

      final override def onEntityUpdate(oldEntity: E, newEntity: E)(implicit db: DBSessionUpdatable): Unit = {
        if (meta.isChanged(oldEntity, newEntity)) {
          val oldEntities = entitiesCreator()
          meta.put(oldEntity, oldEntities)
          traceDown(oldEntity, oldEntities, null)
          val newEntities = entitiesCreator()
          meta.put(newEntity, newEntities)
          traceDown(newEntity, newEntities, null)
          traceUp(oldEntity.id, oldEntities, newEntities)
        }
      }

      class SubTracer[S <: Entity](subMeta: DataView[K, V]#Tracer[ES, E]#SubTracer[S]) extends Tracer[ES, S](subMeta) {
        tracer.addSub(this)

        override val entitiesCreator = tracer.entitiesCreator

        private val index = subMeta.refIndex.asInstanceOf[HostCollectionImpl[E]#HostIndexBaseImpl[_, E]]


        final override val canListenEntityUpdate: Boolean = true
        final override val canListenEntityInsert: Boolean = false
        final override val canListenEntityDelete: Boolean = false

        final override def onEntityInsert(newEntity: S)(implicit db: DBSessionUpdatable): Unit = {
          //doNothing
        }

        final override def onEntityDelete(oldEntity: S)(implicit db: DBSessionUpdatable): Unit = {
          //doNothing
        }

        val target = subMeta.target
        target.addListener(this)

        /**
         * REF or NULL
         */
        @inline final private[common] def getRef(entity: E)(implicit db: DBSessionUpdatable): S = {
          if (entity ne null) {
            val ref = subMeta.getSubRef(entity)
            if (ref ne null) {
              val id = ref.id
              if (id != 0l) {
                return target.impl(id).get
              }
            }
          }
          null.asInstanceOf[S]
        }

        @inline final def traceDown(entity: E, entities: ES)(implicit db: DBSessionUpdatable): Unit = {
          val r = getRef(entity)
          subMeta.put(r, entities)
          traceDown(r, entities, null)
        }

        final override def traceUp(entityID: Long, oldEntities: ES, newEntities: ES)(implicit db: DBSessionUpdatable): Unit = {
          for (u <- index.scanReverse(RefSome[S](entityID))) {
            tracer.meta.put(u, oldEntities)
            tracer.traceDown(u, oldEntities, this)
            tracer.meta.put(u, newEntities)
            tracer.traceDown(u, newEntities, this)
            tracer.traceUp(u.id, oldEntities, newEntities)
          }
        }
      }


      private var subRefs: Array[SubTracer[_]] = null

      @inline final def addSub(sub: SubTracer[_]): Unit = {
        if (subRefs eq null) {
          subRefs = Array[SubTracer[_]](sub)
        } else {
          val l = subRefs.length
          val newSubRefs = new Array[SubTracer[_]](l + 1)
          System.arraycopy(subRefs, 0, newSubRefs, 0, l)
          newSubRefs(l) = sub
          subRefs = newSubRefs
        }
      }

      @inline final def traceDown(entity: E, entities: ES, excludeSubTracer: Tracer[ES, _])(implicit db: DBSessionUpdatable): Unit = {
        if (subRefs != null) {
          var i = subRefs.length - 1
          while (i >= 0) {
            val st = subRefs(i)
            if (st ne excludeSubTracer) {
              st.traceDown(entity, entities)
            }
            i -= 1
          }
        }
      }
    }


    def newMeta[ES <: AnyRef, ROOT <: Entity](meta: DataView[K, V]#Meta[ES, ROOT]) = new DataViewMeta[ES, ROOT](meta)

    class DataViewMeta[ES <: AnyRef, ROOT <: Entity](meta: DataView[K, V]#Meta[ES, ROOT])
      extends Tracer[ES, ROOT](meta) {

      val entitiesCreator = meta.createEntities _

      override final def onEntityInsert(newEntity: ROOT)(implicit db: DBSessionUpdatable): Unit = {
        val newEntities = meta.createEntities()
        meta.put(newEntity, newEntities)
        traceDown(newEntity, newEntities, null)
        for ((k, v) <- meta.extract(newEntities)) {
          db.put(k, v)
        }
      }

      override final def onEntityDelete(oldEntity: ROOT)(implicit db: DBSessionUpdatable): Unit = {
        val oldEntities = meta.createEntities()
        meta.put(oldEntity, oldEntities)
        traceDown(oldEntity, oldEntities, null)
        for ((k, _) <- meta.extract(oldEntities)) {
          db.del(k)
        }
      }

      @inline private final def putKV(k: K, v: V)(implicit db: DBSessionUpdatable) = {
        db.put((getViewID, k), v)(fullKeyPacker, vPacker)
      }

      @inline private final def del(k: K)(implicit db: DBSessionUpdatable) = {
        db.del((getViewID, k))(fullKeyPacker)
      }

      def traceUp(rootEntityID: Long, oldEntities: ES, newEntities: ES)(implicit db: DBSessionUpdatable): Unit = {
        val newKV = meta.extract(oldEntities)
        for ((k, v) <- newKV) {
          putKV(k, v)
        }
        for ((k, _) <- meta.extract(oldEntities)) {
          if (newKV.get(k).isEmpty) {
            del(k)
          }
        }
      }

      final override val canListenEntityUpdate: Boolean = true
      final override val canListenEntityInsert: Boolean = true
      final override val canListenEntityDelete: Boolean = true

      meta.root.addListener(this)
    }

  }

}

