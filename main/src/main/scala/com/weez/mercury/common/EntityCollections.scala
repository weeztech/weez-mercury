package com.weez.mercury.common

import java.util.concurrent.atomic.AtomicReference
import scala.collection.mutable
import scala.reflect.runtime.universe._
import com.weez.mercury.imports.packable
import com.weez.mercury.macros.TuplePrefixed

private object EntityCollections {


  val emptyStringSeq = Seq[String]()
  val emptyLongSet = Set[Long]()

  @inline final def collectionIDOf(entityID: Long): Int = (entityID >>> 48).asInstanceOf[Int]

  @inline final def entityIDOf(collectionID: Int, rawID: Long): Long = (rawID & 0xFFFFFFFFFFFFL) | (collectionID.asInstanceOf[Long] << 48)

  @inline final def getEntity[V <: Entity](id: Long)(implicit db: DBSessionQueryable): V = {
    this.getHost[V](collectionIDOf(id)).get1(id)
  }

  @inline final def getEntityO[V <: Entity](id: Long)(implicit db: DBSessionQueryable): Option[V] = {
    if (id != 0) {
      None
    } else {
      val host = this.findHost[V](collectionIDOf(id))
      if (host eq null) {
        None
      } else {
        host.get0(id)
      }
    }
  }

  def findHost[E <: Entity](collectionID: Int)(implicit db: DBSessionQueryable): HostCollectionImpl[E] = {
    {
      val host = dbObjs(collectionID)
      if (host ne null) {
        host
      } else {
        hosts.synchronized {
          if (hosts.size != bondHostCount) {
            for (h <- hosts.values) {
              h.getMeta
            }
          }
        }
        dbObjs(collectionID)
      }
    }.asInstanceOf[HostCollectionImpl[E]]
  }

  @inline
  final def getHostO[E <: Entity](collectionID: Int)(implicit db: DBSessionQueryable): Option[HostCollectionImpl[E]] = {
    Option(findHost[E](collectionID))
  }

  @inline
  final def getHost[E <: Entity](collectionID: Int)(implicit db: DBSessionQueryable): HostCollectionImpl[E] = {
    val host = findHost[E](collectionID)
    if (host eq null) {
      None.get
    }
    host
  }

  @inline
  final def getDataView[K, V](dataViewID: Int)(implicit db: DBSessionQueryable): DataViewImpl[K, V] = {
    if (dataViewID == FullTextSearchIndex.prefix) {
      return FullTextSearchIndex.asInstanceOf[DataViewImpl[K, V]]
    }
    val dv = dbObjs(dataViewID)
    if (dv.isInstanceOf[DataViewImpl[_, _]]) {
      return dv.asInstanceOf[DataViewImpl[K, V]]
    }
    None.get
  }

  def forSubCollection[O <: Entity, V <: Entity : Packer](pc: SubCollection[O, V]): SubHostCollectionImpl[O, V] = {
    val name = pc.name
    hosts.synchronized {
      hosts.get(name) match {
        case Some(host: SubHostCollectionImpl[O, V]) => host
        case None =>
          val fxFunc = pc.fxFunc.map(f => (sce: SCE[O, V], db: DBSessionQueryable) => f(sce.entity, db))
          val host = new SubHostCollectionImpl[O, V](name, fxFunc.orNull)
          hosts.put(name, host)
          host
        case _ =>
          throw new IllegalArgumentException( s"""SubCollection name conflict :$name""")
      }
    }
  }

  def newHost[V <: Entity : Packer](name: String, getFX: Option[(V, DBSessionQueryable) => Seq[String]]): HostCollectionImpl[V] = {
    hosts.synchronized {
      if (hosts.contains(name)) {
        throw new IllegalArgumentException( s"""HostCollection naming "$name" exist!""")
      }
      val host = new HostCollectionImpl[V](name, getFX.orNull)
      hosts.put(name, host)
      host
    }
  }

  def newDataView[K: Packer, V: Packer](name: String, merger: Merger[V]): DataViewImpl[K, V] = {
    dvs.synchronized {
      if (dvs.contains(name)) {
        throw new IllegalArgumentException( s"""DataView naming "$name" exist!""")
      }
      val dv = new DataViewImpl[K, V](name, merger)
      dvs.put(name, dv)
      dv
    }
  }

  val dvs = collection.mutable.HashMap[String, DataViewImpl[_, _]]()

  val hosts = collection.mutable.HashMap[String, HostCollectionImpl[_]]()
  val dbObjs = new Array[AnyRef](0xFFFF)
  var bondHostCount = 0

  final val RefType = typeOf[Ref[Entity]]

  final val ProductType = typeOf[Product]

  val _emptyListeners = new Array[EntityCollectionListener[_]](0)

  def emptyListeners[V <: Entity] = _emptyListeners.asInstanceOf[Array[EntityCollectionListener[V]]]

  class HostCollectionImpl[V <: Entity](val name: String, val getFX: (V, DBSessionQueryable) => Seq[String])(implicit val valuePacker: Packer[V]) {
    host =>

    @volatile private var _meta: DBType.CollectionMeta = null
    private val listeners = mutable.AnyRefMap[EntityCollectionListener[_], ListenerInfo]()
    @volatile private var deleteListeners = emptyListeners[V]
    @volatile private var updateListeners = emptyListeners[V]
    @volatile private var insertListeners = emptyListeners[V]
    private val indexes = collection.mutable.Map[String, AbstractIndexImpl[V]]()

    final def getMeta(implicit db: DBSessionQueryable): DBType.CollectionMeta = {
      if (_meta == null) {
        synchronized {
          if (_meta ne null) {
            return _meta
          }
          _meta = db.getRootCollectionMeta(name)
          if (_meta == null) {
            throw new IllegalArgumentException( s"""no such HostCollection named $name""")
          }
        }
        hosts.synchronized {
          dbObjs(_meta.prefix) = this
          bondHostCount += 1
        }
        val ims = _meta.indexes
        indexes.synchronized {
          for (idx <- indexes.values if idx._meta eq null) {
            val im = ims.find(_.name == idx.name)
            if (im.isDefined) {
              idx._meta = im.get
            }
          }
        }
      }
      _meta
    }

    @inline final def getCollectionID(implicit db: DBSessionQueryable) = getMeta.prefix

    @inline final def hasRefDefine: Boolean = true //todo 采用Ref信息实现

    final def updateReverseIndex(entityID: Long, oldEntity: V, newEntity: V)(implicit db: DBSessionUpdatable): Unit = {
      //todo 采用Ref信息实现

      def getRefs(e: Any, rs: Set[Long]): Set[Long] = e match {
        case p: Product =>
          var _rs = rs
          var i = p.productArity - 1
          while (i >= 0) {
            _rs = getRefs(p.productElement(i), _rs)
            i -= 1
          }
          _rs
        case r: Ref[_] => rs + r.id
        case _ => rs
      }
      RefReverseIndex.update(entityID, getRefs(oldEntity, emptyLongSet), getRefs(newEntity, emptyLongSet))
    }

    val extractors = new AtomicReference[OuterEntityExtractor[V, _, _]]

    val ftsExtractor = if (getFX eq null) null else new FullTextEntityExtractor(this)

    def updateDataView(entityID: Long, oldEntity: V, newEntity: V)(implicit db: DBSessionUpdatable): Unit = {
      var e = extractors.get
      while (e ne null) {
        e.update(entityID, entityID, oldEntity, newEntity)
        e = e.nextInHost
      }
      if (oldEntity ne null) {
        for (x <- ExtractorIndex.scan(entityID)) {
          val rootEntityID = x._2
          val dataViewID = x._1
          getDataView(dataViewID).update(rootEntityID, entityID, oldEntity, newEntity)
        }
      }
      if (ftsExtractor ne null) {
        ftsExtractor.update(entityID, entityID, oldEntity, newEntity)
      }
    }

    private case class ListenerInfo(l: EntityCollectionListener[V], var refCount: Int)

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
                  override def canListenEntityDelete = listener.canListenEntityDelete

                  override def canListenEntityUpdate = listener.canListenEntityUpdate

                  override def canListenEntityInsert = listener.canListenEntityInsert

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
        deleteListeners = listeners.values.filter(l => l.l.canListenEntityDelete).map(l => l.l).toArray
        updateListeners = listeners.values.filter(l => l.l.canListenEntityUpdate).map(l => l.l).toArray
        insertListeners = listeners.values.filter(l => l.l.canListenEntityInsert).map(l => l.l).toArray
      }
    }

    @inline final def addListener(listener: EntityCollectionListener[V]) =
      this.regListener(listener, None, reg = true)

    @inline final def removeListener(listener: EntityCollectionListener[V]) =
      this.regListener(listener, None, reg = false)

    @inline final def notifyUpdate(oldEntity: V, newEntity: V)(implicit db: DBSessionUpdatable): Unit = {
      val uls = updateListeners
      var i = uls.length - 1
      while (i >= 0) {
        uls(i).onEntityUpdate(oldEntity, newEntity)
        i -= 1
      }
      updateReverseIndex(oldEntity.id, oldEntity, newEntity)
      updateDataView(oldEntity.id, oldEntity, newEntity)
    }

    @inline final def notifyInsert(newEntity: V)(implicit db: DBSessionUpdatable): Unit = {
      val newID = newEntity.id
      val uls = insertListeners
      var i = uls.length - 1
      while (i >= 0) {
        uls(i).onEntityInsert(newEntity)
        i -= 1
      }
      val oldEntity = null.asInstanceOf[V]
      updateReverseIndex(newID, oldEntity, newEntity)
      updateDataView(newID, oldEntity, newEntity)
    }

    @inline final def notifyDelete(id: Long)(implicit db: DBSessionUpdatable): Boolean = {
      val uls = insertListeners
      var i = uls.length - 1
      if (i >= 0 || (getFX ne null) || hasRefDefine) {
        val d = get0(id)
        if (d.isEmpty) {
          return false
        }
        val oldEntity = d.get
        while (i >= 0) {
          uls(i).onEntityDelete(oldEntity)
          i -= 1
        }
        val newEntity = null.asInstanceOf[V]
        updateReverseIndex(id, oldEntity, newEntity)
        updateDataView(id, oldEntity, newEntity)
      }
      true
    }


    @inline final def fixID(id: Long)(implicit db: DBSessionQueryable): Long = entityIDOf(getCollectionID, id)

    //@inline final def fixIDAndGet(id: Long)(implicit db: DBSessionQueryable): Option[V] = db.get(fixID(id))

    @inline final def newEntityID()(implicit db: DBSessionUpdatable): Long = fixID(db.newEntityId())

    @inline final def checkID(id: Long)(implicit db: DBSessionQueryable): Long = {
      if (collectionIDOf(id) != getCollectionID) {
        throw new IllegalArgumentException( s"""id of $id doesn't belong to this collection""")
      }
      id
    }

    @inline final def get1(id: Long)(implicit db: DBSessionQueryable): V = {
      val v = db.get[Long, V](id).get
      v._id = id
      v
    }

    @inline final def get0(id: Long)(implicit db: DBSessionQueryable): Option[V] = {
      val vo = db.get[Long, V](id)
      if (vo.isDefined) {
        vo.get._id = id
      }
      vo
    }

    @inline final def get(id: Long)(implicit db: DBSessionQueryable): Option[V] = {
      if (id == 0) {
        return None
      }
      get0(checkID(id))
    }

    final def scan(forward: Boolean)(implicit db: DBSessionQueryable): Cursor[V] = {
      import Range._
      val cid = getCollectionID
      new RawKVCursor[V](entityIDOf(cid, 0) +-+ entityIDOf(cid, -1L), forward) {
        override def buildValue(): V = {
          val entity = valuePacker.unapply(rawValue)
          entity._id = Packer.LongPacker.unapply(rawKey)
          entity
        }
      }
    }

    final def insert(value: V)(implicit db: DBSessionUpdatable): Long = {
      val id = this.newEntityID()
      value._id = id
      notifyInsert(value)
      db.put(id, value)
      id
    }

    final def update(value: V)(implicit db: DBSessionUpdatable): Unit = {
      if (value._id == 0l) {
        insert(value)
        return
      }
      val id = checkID(value._id)
      val old = db.get[Long, V](id)
      if (old.isEmpty) {
        throw new IllegalArgumentException( s"""entity who's id is $id doesn't exist""")
      }
      notifyUpdate(old.get, value)
      db.put(id, value)
    }

    final def delete(id: Long)(implicit db: DBSessionUpdatable): Unit = {
      if (notifyDelete(checkID(id))) {
        db.del(id)
      }
    }

    final def newIndex[INDEX <: AbstractIndexImpl[V]](indexName: String)(f: => INDEX): INDEX = {
      indexes.synchronized {
        if (indexes.get(indexName).isDefined) {
          throw new IllegalArgumentException( s"""index naming "$name" exist!""")
        }
        val idx = f
        this.indexes.put(indexName, idx)
        idx
      }
    }

    final def getOrNewIndex[INDEX <: AbstractIndexImpl[V]](indexName: String)(f: => INDEX): INDEX = {
      indexes.synchronized {
        val idxO = indexes.get(indexName)
        if (idxO.isDefined) {
          idxO.get.asInstanceOf[INDEX]
        }
        val idx = f
        this.indexes.put(indexName, idx)
        idx
      }
    }

    final def defUniqueIndex[K: Packer](indexName: String, keyGetter: V => K): UniqueIndex[K, V] = {
      type FullKey = (Int, K)
      newIndex(indexName) {
        new UniqueIndexImpl[FullKey, V](this, indexName) with UniqueIndex[K, V] {

          override def v2fk(value: V)(implicit db: DBSessionQueryable) = (this.getIndexID, keyGetter(value))

          @inline override final def apply(key: K)(implicit db: DBSessionQueryable): Option[V] = {
            db.get[FullKey, Long](this.getIndexID, key).flatMap(db.get[Long, V])
          }

          @inline override final def delete(key: K)(implicit db: DBSessionUpdatable): Unit = {
            db.get[FullKey, Long](this.getIndexID, key).foreach(host.delete)
          }

          @inline override final def update(value: V)(implicit db: DBSessionUpdatable): Unit = {
            host.update(value)
          }

          override def apply()(implicit db: DBSessionQueryable): Cursor[V] = {
            import Range._
            Cursor[Long](Tuple1(getIndexID).asRange, forward = true).map(id => host.get1(id))
          }

          override def apply[P: Packer](range: Range[P], forward: Boolean)(implicit db: DBSessionQueryable, canUse: TuplePrefixed[K, P]): Cursor[V] = {
            val r = if (canUse.shouldTuple)
              range.map(r => (getIndexID, Tuple1(r)))
            else
              range.map(r => (getIndexID, r))
            Cursor[Long](r, forward).map(id => host.get1(id))
          }
        }
      }
    }

    final def defMKIndex[K: Packer](indexName: String, keyGetter: V => Seq[K]): Index[K, V] = {
      ???
    }

    final def defIndex[K: Packer](indexName: String, keyGetter: V => K): Index[K, V] = {
      type FullKey = (Int, K, Long)
      newIndex(indexName) {
        new IndexImpl[FullKey, V](this, name) with Index[K, V] {
          override def v2fk(value: V)(implicit db: DBSessionQueryable) = (this.getIndexID, keyGetter(value), value.id)

          override def apply()(implicit db: DBSessionQueryable): Cursor[V] = {
            import Range._
            Cursor[Long](Tuple1(getIndexID).asRange, forward = true).map(id => host.get1(id))
          }

          override def apply[P: Packer](range: Range[P], forward: Boolean)(implicit db: DBSessionQueryable, canUse: TuplePrefixed[K, P]): Cursor[V] = {
            val r = if (canUse.shouldTuple)
              range.map(r => (getIndexID, Tuple1(r)))
            else
              range.map(r => (getIndexID, r))
            Cursor[Long](r, forward).map(id => host.get1(id))
          }
        }
      }
    }
  }

  @packable
  case class SCE[O <: Entity, V <: Entity](owner: Ref[O], entity: V) extends Entity with SubEntityPair[O, V] {
    this._id = entity.id
  }

  class SubHostCollectionImpl[O <: Entity, V <: Entity : Packer](name: String, getFX: (SCE[O, V], DBSessionQueryable) => Seq[String])
    extends HostCollectionImpl[SCE[O, V]](name, getFX) {
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

    trait SubIndex[K] extends AbstractIndexImpl[SCE[O, V]]

    def defUniqueIndex[K: Packer](owner: Ref[O], indexName: String, keyGetter: V => K): UniqueIndex[K, V] = {
      type FullKey = (Int, Long, K)
      val rawIndex = getOrNewIndex(indexName) {
        new UniqueIndexImpl[FullKey, SCE[O, V]](this, indexName) with SubIndex[K] {
          override def v2fk(value: SCE[O, V])(implicit db: DBSessionQueryable) = (this.getIndexID, value.owner.id, keyGetter(value.entity))
        }
      }
      new UniqueIndex[K, V] {
        override def update(value: V)(implicit db: DBSessionUpdatable): Unit = {
          subHost.update(SCE(owner, value))
        }

        override def delete(key: K)(implicit db: DBSessionUpdatable): Unit = {
          rawIndex.deleteByFullKey((rawIndex.getIndexID, owner.id, key))
        }

        override def apply(key: K)(implicit db: DBSessionQueryable): Option[V] = {
          rawIndex.getByFullKey(rawIndex.getIndexID, owner.id, key).map(_.entity)
        }

        override def apply()(implicit db: DBSessionQueryable): Cursor[V] = {
          import Range._
          Cursor[Long]((rawIndex.getIndexID, owner.id).asRange, forward = true).map(id => subHost.get1(id).entity)
        }

        override def apply[P: Packer](range: Range[P], forward: Boolean)(implicit db: DBSessionQueryable, canUse: TuplePrefixed[K, P]): Cursor[V] = {
          val r = if (canUse.shouldTuple)
            range.map(r => (rawIndex.getIndexID, owner.id, Tuple1(r)))
          else
            range.map(r => (rawIndex.getIndexID, owner.id, r))
          Cursor[Long](r, forward).map(id => subHost.get1(id).entity)
        }
      }
    }

    final def defMKIndex[K: Packer](owner: Ref[O], indexName: String, keyGetter: V => Seq[K]): Index[K, V] = {
      ???
    }

    final def defIndex[K: Packer](owner: Ref[O], indexName: String, keyGetter: V => K): Index[K, V] = {
      type FullKey = (Int, Long, K, Long)
      val rawIndex = getOrNewIndex(indexName) {
        new IndexImpl[FullKey, SCE[O, V]](this, indexName) with SubIndex[K] {
          override def v2fk(value: SCE[O, V])(implicit db: DBSessionQueryable) = (this.getIndexID, value.owner.id, keyGetter(value.entity), value.id)
        }
      }
      new Index[K, V] {
        override def apply()(implicit db: DBSessionQueryable): Cursor[V] = {
          import Range._
          new RawKVCursor[V]((rawIndex.getIndexID, owner.id).asRange, forward = true) {
            override def buildValue(): V = {
              subHost.get1(rawIndex.fullKeyPacker.unapply(rawKey)._4).entity
            }
          }
        }

        override def apply[P: Packer](range: Range[P], forward: Boolean)(implicit db: DBSessionQueryable, canUse: TuplePrefixed[K, P]): Cursor[V] = {
          val r = if (canUse.shouldTuple)
            range.map(r => (rawIndex.getIndexID, owner.id, Tuple1(r)))
          else
            range.map(r => (rawIndex.getIndexID, owner.id, r))
          new RawKVCursor[V](r, forward = true) {
            override def buildValue(): V = {
              subHost.get1(rawIndex.fullKeyPacker.unapply(rawKey)._4).entity
            }
          }
        }
      }
    }
  }

  class RefTracer(db: DBSessionQueryable, rootEntityID: Long, refEntityID: Long, oldRefEntity: Entity, newRefEntity: Entity) extends DBSessionQueryable {
    val oldRefIDs = mutable.HashSet.empty[Long]
    val newRefIDs = mutable.HashSet.empty[Long]
    val oldRef = Option(oldRefEntity)
    val newRef = Option(newRefEntity)
    private var ref = oldRef
    private var refIDs = oldRefIDs

    @inline
    final def use(asNew: Boolean): Unit = {
      if (asNew) {
        ref = newRef
        refIDs = newRefIDs
      } else {
        ref = oldRef
        refIDs = oldRefIDs
      }
    }

    def get[K: Packer, V: Packer](key: K): Option[V] = key match {
      case `refEntityID` => ref.asInstanceOf[Option[V]]
      case id: Long =>
        if (id != rootEntityID) {
          refIDs += id
        }
        db.get[Long, V](id)
      case _ => db.get[K, V](key)
    }

    def exists[K: Packer](key: K): Boolean = db.exists(key)

    def newCursor(): DBCursor = db.newCursor()

    def getRootCollectionMeta(name: String)(implicit db: DBSessionQueryable): DBType.CollectionMeta = db.getRootCollectionMeta(name)

  }


  type Extract[S, DK, DV] = (S, DBSessionQueryable) => scala.collection.Map[DK, DV]
  type Extract2[K, V, DK, DV] = (K, V, DBSessionQueryable) => scala.collection.Map[DK, DV]

  class DataViewExtractor[K, V, K2, V2](val from: DataViewImpl[K, V], to: DataViewImpl[K2, V2], extract: Extract2[K, V, K2, V2]) {
    def update(k: K, oldV: Option[V], newV: Option[V], tracer: RefTracer)(implicit db: DBSessionUpdatable): Unit = {
      def ex(v: Option[V], asNew: Boolean) = {
        tracer.use(asNew)
        if (v.isEmpty) Map.empty.asInstanceOf[scala.collection.Map[K2, V2]] else extract(k, v.get, tracer)
      }
      to.updateDB(ex(oldV, asNew = false), ex(oldV, asNew = true), tracer, 0)
    }

    if (to.findExtractor(from) ne null) {
      throw new IllegalArgumentException("dump extractor")
    }
    private var _nextInDV = to.dataViewExtractors.get()
    while (!to.dataViewExtractors.compareAndSet(_nextInDV, this)) {
      _nextInDV = to.dataViewExtractors.get()
    }
    private var _nextInTracing = from.tracingDataViewExtractors.get
    while (!from.tracingDataViewExtractors.compareAndSet(_nextInTracing, this)) {
      _nextInTracing = from.tracingDataViewExtractors.get()
    }

    @inline final def nextInDV = _nextInDV

    @inline final def nextInTracing = _nextInTracing

  }


  class EntityExtractor[E <: Entity, DK, DV](val dv: DataViewImpl[DK, DV], val host: HostCollectionImpl[E], extract: Extract[E, DK, DV]) {
    def update(rootEntityID: Long, refEntityID: Long, oldEntity: Entity, newEntity: Entity)(implicit db: DBSessionUpdatable): Unit = {
      val oldRoot: E = if (rootEntityID == refEntityID) oldEntity.asInstanceOf[E] else host.get1(rootEntityID)
      val newRoot: E = if (rootEntityID == refEntityID) newEntity.asInstanceOf[E] else oldRoot
      val tracer = new RefTracer(db, rootEntityID, refEntityID, oldEntity, newEntity)
      val oldKV = if (oldRoot ne null) extract(oldRoot, tracer) else Map.empty[DK, DV]
      tracer.use(asNew = true)
      val newKV = if (newRoot ne null) extract(newRoot, tracer) else Map.empty[DK, DV]
      dv.updateDB(oldKV, newKV, tracer, rootEntityID)
    }
  }

  class OuterEntityExtractor[E <: Entity, DK, DV](dv: DataViewImpl[DK, DV],
                                                  host: HostCollectionImpl[E],
                                                  extract: Extract[E, DK, DV])
    extends EntityExtractor[E, DK, DV](dv, host, extract) {

    if (dv.findExtractor(host) ne null) {
      throw new IllegalArgumentException("dump extractor")
    }
    private var _nextInDV = dv.entityExtractors.get()
    while (!dv.entityExtractors.compareAndSet(_nextInDV, this)) {
      _nextInDV = dv.entityExtractors.get()
    }
    private var _nextInHost = host.extractors.get
    while (!host.extractors.compareAndSet(_nextInHost, this)) {
      _nextInHost = host.extractors.get()
    }

    @inline final def nextInDV = _nextInDV

    @inline final def nextInHost = _nextInHost
  }

  class FullTextEntityExtractor[E <: Entity](host: HostCollectionImpl[E])
    extends EntityExtractor[E, (String, Long), Boolean](FullTextSearchIndex, host,
      (v, db) => FullTextSearch.splitInternal(v.id, host.getFX(v, db): _*))

  class DataViewImpl[DK: Packer, DV: Packer](val name: String, val merger: Merger[DV])
                                            (implicit val fullKeyPacker: Packer[(Int, DK)], val vPacker: Packer[DV]) {

    val entityExtractors = new AtomicReference[OuterEntityExtractor[_, DK, DV]](null)

    def defExtractor[E <: Entity](collection: EntityCollection[E], f: Extract[E, DK, DV]): Unit = {
      collection match {
        case r: RootCollection[E] =>
          new OuterEntityExtractor(this, r.impl, f)
        case s: SubCollection[_, E] =>
          val ex = (sce: SCE[_, E], db: DBSessionQueryable) => f(sce.entity, db)
          new OuterEntityExtractor(this, s.host, ex)
      }
    }

    def findExtractor[E <: Entity](host: HostCollectionImpl[E]): EntityExtractor[E, DK, DV] = {
      var e = entityExtractors.get()
      while (e ne null) {
        if (e.host eq host) {
          return e.asInstanceOf[EntityExtractor[E, DK, DV]]
        }
        e = e.nextInDV
      }
      null
    }

    val dataViewExtractors = new AtomicReference[DataViewExtractor[_, _, DK, DV]](null)
    val tracingDataViewExtractors = new AtomicReference[DataViewExtractor[DK, DV, _, _]](null)

    def defExtractor[K, V](dataView: DataViewImpl[K, V], f: Extract2[K, V, DK, DV]): Unit = {
      new DataViewExtractor(dataView, this, f)
    }

    def findExtractor[K, V](dataView: DataViewImpl[K, V]): DataViewExtractor[K, V, DK, DV] = {
      var e = dataViewExtractors.get()
      while (e ne null) {
        if (e.from eq dataView) {
          return e.asInstanceOf[DataViewExtractor[K, V, DK, DV]]
        }
        e = e.nextInDV
      }
      null
    }

    def update[E <: Entity](rootEntityID: Long, refEntityID: Long, oldEntity: E, newEntity: E)(implicit db: DBSessionUpdatable): Unit = {
      val extractor = findExtractor[E](getHost[E](collectionIDOf(rootEntityID)))
      if (extractor ne null) {
        extractor.update(rootEntityID, refEntityID, oldEntity, newEntity)
      }
    }

    def getDataViewID(implicit db: DBSessionQueryable): Int = ???

    final def updateDB(oldKV: collection.Map[DK, DV], newKV: collection.Map[DK, DV], tracer: RefTracer, rootEntityID: Long)
                      (implicit db: DBSessionUpdatable): Unit = {
      val dataViewID = getDataViewID
      def get(k: DK): Option[DV] = db.get((dataViewID, k))(fullKeyPacker, vPacker)
      def put(k: DK, v: DV) = db.put((dataViewID, k), v)(fullKeyPacker, vPacker)
      def del(k: DK) = db.del((dataViewID, k))(fullKeyPacker)
      def update(k: DK, oldV: Option[DV], newV: Option[DV]): Unit = {
        if (oldV.isEmpty && newV.isEmpty) {
          return
        }
        var extractor = this.tracingDataViewExtractors.get
        while (extractor ne null) {
          extractor.update(k, oldV, newV, tracer)
          extractor = extractor.nextInTracing
        }
      }
      for ((k, v) <- newKV) {
        val oldV = oldKV.get(k)
        if (oldV.isEmpty || oldV.get != v) {
          if (merger ne null) {
            val m1 = merger.sub(v, oldV.get)
            if (m1.isDefined) {
              val org = get(k)
              if (org.isEmpty) {
                put(k, m1.get)
                update(k, org, m1)
              } else {
                val m2 = merger.add(org.get, m1.get)
                if (m2.isEmpty) {
                  del(k)
                } else {
                  put(k, m2.get)
                }
                update(k, org, m2)
              }
            }
          } else {
            put(k, v)
            update(k, oldV, Some(v))
          }
        }
      }
      for ((k, v) <- oldKV -- newKV.keys) {
        if (merger ne null) {
          val org = get(k)
          if (org.isEmpty) {
            put(k, v)
            update(k, org, Some(v))
          } else {
            val m1 = merger.sub(org.get, v)
            if (m1.isEmpty) {
              del(k)
            } else {
              put(k, m1.get)
            }
            update(k, org, m1)
          }
        } else {
          del(k)
          update(k, None, Some(v))
        }
      }
      if (rootEntityID != 0) {
        ExtractorIndex.update(dataViewID, rootEntityID, tracer.oldRefIDs, tracer.newRefIDs)
      }
    }

    final def scan(dbRange: DBRange, forward: Boolean = true)(implicit db: DBSessionQueryable): Cursor[KeyValue[DK, DV]] = {
      new RawKVCursor[KeyValue[DK, DV]](dbRange, forward) {
        override def buildValue() = {
          val rk = fullKeyPacker.unapply(rawKey)
          val rv = vPacker.unapply(rawValue)
          KeyValue[DK, DV](rk._2, rv)
        }
      }
    }

    @inline final def apply(forward: Boolean)(implicit db: DBSessionQueryable): Cursor[KeyValue[DK, DV]] = {
      import Range._
      val prefix = Tuple1(getDataViewID)
      scan(prefix +-+ prefix, forward)
    }

    @inline final def apply[P: Packer](range: Range[P], forward: Boolean = true)(implicit db: DBSessionQueryable, canUse: TuplePrefixed[DK, P]): Cursor[KeyValue[DK, DV]] = {
      val dvID = getDataViewID
      val r = if (canUse.shouldTuple) range.map(r => (dvID, Tuple1(r))) else range.map(r => (dvID, r))
      scan(r, forward)
    }
  }

  trait HostListening[E <: Entity] extends EntityCollectionListener[E] {
    val host: HostCollectionImpl[E]
    host.addListener(this)
  }

  trait AbstractIndexImpl[E <: Entity] {
    val host: HostCollectionImpl[E]
    val name: String
    var _meta: DBType.IndexMeta = null

    @inline final def getIndexID(implicit db: DBSessionQueryable) = {
      if (_meta eq null) {
        val hm = host.getMeta
        if (_meta eq null) {
          _meta = hm.indexes.find(_.name == name).get
        }
      }
      _meta.prefix
    }
  }

  abstract class UniqueIndexImpl[FK, V <: Entity](val host: HostCollectionImpl[V], val name: String)(implicit fullKeyPacker: Packer[FK])
    extends AbstractIndexImpl[V] with HostListening[V] {

    def v2fk(value: V)(implicit db: DBSessionQueryable): FK

    @inline final def deleteByFullKey(fullKey: FK)(implicit db: DBSessionUpdatable): Unit = {
      val id = db.get[FK, Long](fullKey)
      if (id.isDefined) {
        host.delete(id.get)
      }
    }

    @inline final def getByFullKey(fullKey: FK)(implicit db: DBSessionQueryable): Option[V] = {
      val id = db.get[FK, Long](fullKey)
      if (id.isDefined) host.get0(id.get) else None
    }

    override def onEntityInsert(newEntity: V)(implicit db: DBSessionUpdatable): Unit = {
      db.put(v2fk(newEntity), newEntity.id)
    }

    override def onEntityDelete(oldEntity: V)(implicit db: DBSessionUpdatable): Unit = {
      db.del(v2fk(oldEntity))
    }

    override def onEntityUpdate(oldEntity: V, newEntity: V)(implicit db: DBSessionUpdatable): Unit = {
      val oldIndexEntryKey = v2fk(oldEntity)
      val newIndexEntryKey = v2fk(newEntity)
      if (oldIndexEntryKey != newIndexEntryKey) {
        db.del(oldIndexEntryKey)
        db.put(newIndexEntryKey, newEntity.id)
      }
    }

  }

  abstract class IndexImpl[FK, V <: Entity : Packer](val host: HostCollectionImpl[V], val name: String)(implicit val fullKeyPacker: Packer[FK])
    extends AbstractIndexImpl[V] with HostListening[V] {
    index =>

    def v2fk(value: V)(implicit db: DBSessionQueryable): FK

    override def onEntityInsert(newEntity: V)(implicit db: DBSessionUpdatable): Unit = {
      db.put(v2fk(newEntity), true)
    }

    override def onEntityDelete(oldEntity: V)(implicit db: DBSessionUpdatable): Unit = {
      db.del(v2fk(oldEntity))
    }

    override def onEntityUpdate(oldEntity: V, newEntity: V)(implicit db: DBSessionUpdatable): Unit = {
      val oldIndexEntryKey = v2fk(oldEntity)
      val newIndexEntryKey = v2fk(newEntity)
      if (oldIndexEntryKey != newIndexEntryKey) {
        db.del(oldIndexEntryKey)
        db.put(newIndexEntryKey, true)
      }
    }

  }

  object RefReverseIndex {
    final val prefix = Int.MaxValue - 1
    implicit val fullKeyPacker = Packer.of[(Int, Long, Long)]

    @inline final def update(entityID: Long, oldRefs: Set[Long], newRefs: Set[Long])(implicit db: DBSessionUpdatable): Unit = {
      for (r <- newRefs) {
        if (!oldRefs.contains(r)) {
          db.put((prefix, r, entityID), true)
        }
      }
      for (r <- oldRefs) {
        if (!newRefs.contains(r)) {
          db.del((prefix, r, entityID))
        }
      }
    }

    @inline final def scanRefID(id: Long, cidFrom: Int, cidTo: Int)(implicit db: DBSessionQueryable): Cursor[Long] = {
      import Range._
      new RawKVCursor[Long]((prefix, id, entityIDOf(cidFrom, 0)) +-+(prefix, id, entityIDOf(cidTo, Long.MaxValue)), forward = true) {
        override def buildValue() = {
          fullKeyPacker.unapply(rawKey)._3
        }
      }
    }

    @inline final def scan[V <: Entity](id: Long, host: HostCollectionImpl[V])(implicit db: DBSessionQueryable): Cursor[V] = {
      val (minCid, maxCid) = if (host eq null) (0, Int.MaxValue) else (host.getCollectionID, host.getCollectionID)
      scanRefID(id, minCid, maxCid).map(id => getEntity[V](id))
    }
  }

  object FullTextSearchIndex extends DataViewImpl[(String, Long), Boolean]("full-text-search", null) {

    final val prefix = Int.MaxValue - 2

    override def getDataViewID(implicit db: DBSessionQueryable) = prefix

    override def findExtractor[E <: Entity](host: HostCollectionImpl[E]) = host.ftsExtractor


    @inline final def scan[V <: Entity](word: String, host: HostCollectionImpl[V])(implicit db: DBSessionQueryable): Cursor[V] = {
      import Range._
      val r = if (host eq null) {
        (getDataViewID, (word, 0l)) +-+(getDataViewID, (word, Long.MaxValue))
      } else {
        (getDataViewID, (word, host.fixID(0))) +-+(getDataViewID, (word, host.fixID(Long.MaxValue)))
      }
      new RawKVCursor[V](r, forward = true) {
        override def buildValue() = {
          val fk = fullKeyPacker.unapply(rawKey)
          getEntity[V](fk._2._2)
        }
      }
    }
  }

  object ExtractorIndex {
    final val prefix = Int.MaxValue - 3
    implicit val fullKeyPacker = Packer.of[(Int, Long, Int, Long)]

    def update(dataViewID: Int, rootEntityID: Long, oldRefIDs: collection.Set[Long], newRefIDs: collection.Set[Long])(implicit db: DBSessionUpdatable): Unit = {
      for (rid <- newRefIDs) {
        if (!oldRefIDs.contains(rid)) {
          db.put((prefix, rid, dataViewID, rootEntityID), true)
        }
      }
      for (rid <- oldRefIDs) {
        if (!newRefIDs.contains(rid)) {
          db.del((prefix, rid, dataViewID, rootEntityID))
        }
      }
    }

    @inline final def scan(refID: Long)(implicit db: DBSessionQueryable): Cursor[(Int, Long)] = {
      import Range._
      new RawKVCursor[(Int, Long)]((prefix, refID, 0, 0L) +--(prefix, refID, Int.MaxValue, Long.MaxValue), forward = true) {
        override def buildValue() = {
          val rv = fullKeyPacker.unapply(rawKey)
          (rv._3, rv._4)
        }
      }
    }
  }

  abstract class RawKVCursor[V](range: DBRange, forward: Boolean)(implicit db: DBSessionQueryable) extends Cursor[V] {
    self =>

    import Range.{ByteArrayOrdering => O}

    private val step = if (forward) 1 else -1
    private val rangeStart = range.keyStart
    private val rangeEnd = range.keyEnd
    private var dbCursor: DBCursor =
      if (O.compare(rangeStart, rangeEnd) < 0) {
        val c = db.newCursor()
        if (forward)
          c.seek(rangeStart)
        else {
          c.seek(rangeEnd)
          if (c.isValid) c.next(-1)
        }
        c
      } else null

    private var rawKeyExpired = true
    private var valueExpired = true
    private var _rawKey: Array[Byte] = null

    @inline final def rawKey = {
      if (rawKeyExpired) {
        _rawKey = dbCursor.key()
        rawKeyExpired = false
      }
      _rawKey
    }

    @inline final def rawValue = dbCursor.value()

    final def isValid: Boolean = {
      dbCursor != null && {
        val valid =
          dbCursor.isValid && {
            if (forward)
              O.compare(rawKey, rangeEnd) < 0
            else
              O.compare(rawKey, rangeStart) >= 0
          }
        if (!valid) close()
        valid
      }
    }

    def buildValue(): V

    private var _value: V = _

    override final def value: V = {
      if (valueExpired) {
        _value = buildValue()
        valueExpired = false
      }
      _value
    }


    final def next() = {
      if (dbCursor != null) {
        dbCursor.next(step)
        rawKeyExpired = true
        valueExpired = true
      }
    }

    final def close() = {
      if (dbCursor != null) {
        dbCursor.close()
        rawKeyExpired = true
        valueExpired = true
        dbCursor = null
      }
    }
  }

}