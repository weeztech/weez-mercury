package com.weez.mercury.common

import java.util.concurrent.atomic.AtomicReference

import scala.collection.mutable
import com.weez.mercury.imports.packable
import com.weez.mercury.macros.TuplePrefixed


private object EntityCollections {


  trait DBObject {
    def dbBind()(implicit db: DBSessionQueryable)
  }

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


  @inline final def findHost[E <: Entity](collectionID: Int)(implicit db: DBSessionQueryable): HostCollectionImpl[E] = {
    findDBObject(collectionID).asInstanceOf[HostCollectionImpl[E]]
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

  def getCollectionName(c: EntityCollection[_]): String = {
    val sn = c.getClass.getSimpleName
    var i = sn.length - 1
    while (sn.charAt(i) == '$') {
      i -= 1
    }
    Util.camelCase2seqStyle(sn.substring(0, i + 1))
  }

  def forSubCollection[O <: Entity, V <: Entity : Packer](sc: SubCollection[O, V]): SubHostCollectionImpl[O, V] = {
    val name = getCollectionName(sc)
    dbObjectsByName synchronized {
      dbObjectsByName.get(name) match {
        case Some(host: SubHostCollectionImpl[O, V]) => host
        case None =>
          val host = new SubHostCollectionImpl[O, V](name)
          dbObjectsByName.put(name, host)
          host
        case _ =>
          throw new IllegalArgumentException( s"""SubCollection name conflict :$name""")
      }
    }
  }

  def newHost[V <: Entity : Packer](rc: RootCollection[V]): HostCollectionImpl[V] = {
    val name = getCollectionName(rc)
    dbObjectsByName synchronized {
      if (dbObjectsByName.contains(name)) {
        throw new IllegalArgumentException( s"""HostCollection naming "$name" exist!""")
      }
      val host = new HostCollectionImpl[V](name)
      dbObjectsByName.put(name, host)
      host
    }
  }

  def newDataView[K: Packer, V: Packer](name: String, merger: Merger[V]): DataViewImpl[K, V] = {
    dbObjectsByName synchronized {
      if (dbObjectsByName.contains(name)) {
        throw new IllegalArgumentException( s"""DataView naming "$name" exist!""")
      }
      val dv = new DataViewImpl[K, V](name, merger)
      dbObjectsByName.put(name, dv)
      dv
    }
  }

  private val dbObjectsByName = collection.mutable.HashMap[String, DBObject]()
  private var dbObjectBondedCount = 0
  private val dbObjects = new Array[DBObject](0xFFFF)

  def bindDBObj(index: Int, dbObj: DBObject): Unit = {
    dbObjects synchronized {
      val old = dbObjects(index)
      if (old eq dbObj) {
        return
      }
      if (old eq null) {
        dbObjects(index) = dbObj
        dbObjectBondedCount += 1
        return
      }
    }
    throw new IllegalArgumentException(s"dbObjects($index) already binded!")
  }

  def findDBObject(id: Int)(implicit db: DBSessionQueryable): DBObject = {
    val obj = dbObjects(id)
    if (obj ne null) {
      return obj
    }
    dbObjectsByName synchronized {
      if (dbObjectsByName.size != dbObjectBondedCount) {
        for (obj <- dbObjectsByName.values) {
          obj.dbBind()
        }
      }
    }
    dbObjects(id)
  }


  val _emptyListeners = new Array[EntityCollectionListener[_]](0)

  def emptyListeners[V <: Entity] = _emptyListeners.asInstanceOf[Array[EntityCollectionListener[V]]]

  class HostCollectionImpl[V <: Entity](val name: String)(implicit val valuePacker: Packer[V]) extends DBObject {
    host =>

    @volatile private var _meta: DBType.CollectionMeta = null
    private val listeners = mutable.AnyRefMap[EntityCollectionListener[_], ListenerInfo]()
    @volatile private var deleteListeners = emptyListeners[V]
    @volatile private var updateListeners = emptyListeners[V]
    @volatile private var insertListeners = emptyListeners[V]
    val indexes = new AtomicReference[AbstractIndexImpl[_, V]]()


    final def dbBind()(implicit db: DBSessionQueryable): Unit = {
      if (_meta ne null) {
        return
      }
      synchronized {
        if (_meta ne null) {
          return
        }
        db.getMeta(name) match {
          case x: DBType.CollectionMeta => _meta = x
          case x => throw new IllegalStateException(s"expect collection meta, but found $x")
        }
      }
      bindDBObj(_meta.prefix, this)
      var idx = indexes.get()
      while (idx ne null) {
        idx.dbBind()
        idx = idx.nextInHost
      }
    }

    @inline
    final def getMeta(implicit db: DBSessionQueryable): DBType.CollectionMeta = {
      if (_meta ne null) {
        return _meta
      }
      dbBind()
      _meta
    }

    @inline final def getCollectionID(implicit db: DBSessionQueryable) = getMeta.prefix

    @inline final def hasRefFields: Boolean = true

    @inline final def hasStringFields: Boolean = true

    val extractors = new AtomicReference[EntityExtractor[V, _, _]]

    private var hasReverseExtracts = 0

    @inline final def setHasReverseExtracts() = hasReverseExtracts = 1

    private final def updateInternals(entityID: Long, oldEntity: V, newEntity: V)(implicit db: DBSessionUpdatable): Unit = {
      //全文检索和反向索引
      var oldRefs = Set.empty[Long]
      var newRefs = Set.empty[Long]
      var oldTexts = List.empty[String]
      var newTexts = List.empty[String]

      def search(e: Any, isNew: Boolean): Unit = e match {
        case p: Product =>
          var i = p.productArity - 1
          while (i >= 0) {
            search(p.productElement(i), isNew)
            i -= 1
          }
        case p: Iterable[_] =>
          p.foreach(e => search(e, isNew))
        case r: Ref[_] => if (r.isDefined) if (isNew) newRefs += r.id else oldRefs += r.id
        case s: String => if (s.length > 0) if (isNew) newTexts ::= s else oldTexts ::= s
        case _ =>
      }
      search(oldEntity, isNew = false)
      search(newEntity, isNew = true)
      FullTextSearchIndex.update(entityID, oldTexts, newTexts)
      RefReverseIndex.update(entityID, oldRefs, newRefs)
      //索引
      var idx = indexes.get
      while (idx ne null) {
        idx.update(entityID, oldEntity, newEntity)
        idx = idx.nextInHost
      }
      //视图extractor
      var e = extractors.get
      while (e ne null) {
        e.update(entityID, oldEntity, newEntity)
        e = e.nextInHost
      }
      if (hasReverseExtracts == 0) {
        if (ExtractorReverseIndex.hasReverseExtracts(getCollectionID)) {
          hasReverseExtracts = 1
        } else {
          hasReverseExtracts = -1
        }
      }
      if (hasReverseExtracts > 0) {
        val cursor = ExtractorReverseIndex.scan(entityID)
        try {
          for (x <- cursor) {
            findDBObject(x.extractID) match {
              case dv: DataViewImpl[_, _] => dv.update(x.rootEntityID, entityID, oldEntity, newEntity)
              case idx: AbstractIndexImpl[_, _] => idx.update(x.rootEntityID, entityID, oldEntity, newEntity)
              case _ => throw new IllegalStateException(s"extractor missing! id: ${x.extractID}")
            }
          }
        } finally {
          cursor.close()
        }
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

    @inline
    final def addListener(listener: EntityCollectionListener[V]) =
      this.regListener(listener, None, reg = true)

    @inline
    final def removeListener(listener: EntityCollectionListener[V]) =
      this.regListener(listener, None, reg = false)

    @inline
    final def notifyUpdate(oldEntity: V, newEntity: V)(implicit db: DBSessionUpdatable): Unit = {
      val uls = updateListeners
      var i = uls.length - 1
      while (i >= 0) {
        uls(i).onEntityUpdate(oldEntity, newEntity)
        i -= 1
      }
      updateInternals(oldEntity.id, oldEntity, newEntity)
    }

    @inline
    final def notifyInsert(newEntity: V)(implicit db: DBSessionUpdatable): Unit = {
      val newID = newEntity.id
      val uls = insertListeners
      var i = uls.length - 1
      while (i >= 0) {
        uls(i).onEntityInsert(newEntity)
        i -= 1
      }
      updateInternals(newID, null.asInstanceOf[V], newEntity)
    }

    @inline
    final def notifyDelete(id: Long)(implicit db: DBSessionUpdatable): Boolean = {
      val d = get0(id)
      if (d.isEmpty) {
        return false
      }
      val oldEntity = d.get
      val uls = insertListeners
      var i = uls.length - 1
      while (i >= 0) {
        uls(i).onEntityDelete(oldEntity)
        i -= 1
      }
      updateInternals(id, oldEntity, null.asInstanceOf[V])
      true
    }


    @inline final def fixID(id: Long)(implicit db: DBSessionQueryable): Long = entityIDOf(getCollectionID, id)

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

    @inline final def contains(id: Long)(implicit db: DBSessionQueryable): Boolean = {
      if (id == 0) {
        false
      } else {
        db.exists(checkID(id))
      }
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

    final def findIndex(indexName: String): AbstractIndexImpl[_, V] = {
      var idx = indexes.get()
      while (idx ne null) {
        if (idx.name == indexName) {
          return idx
        }
        idx = idx.nextInHost
      }
      null
    }

    final def newIndex[INDEX <: AbstractIndexImpl[_, V]](indexName: String)(f: => INDEX): INDEX = {
      indexes synchronized {
        if (findIndex(indexName) ne null) {
          throw new IllegalArgumentException( s"""index naming "$indexName" exist!""")
        }
        f
      }
    }

    final def getOrNewIndex[INDEX <: AbstractIndexImpl[_, V]](indexName: String)(f: => INDEX): INDEX = {
      synchronized {
        val idxO = findIndex(indexName)
        if (idxO ne null) {
          idxO.asInstanceOf[INDEX]
        }
        f
      }
    }

    final def defUniqueIndex[K: Packer](indexName: String, keyGetter: V => K): UniqueIndex[K, V] = {
      defUniqueIndexEx[K](indexName, (v, db) => Set.empty[K] + keyGetter(v))
    }

    final def defUniqueIndexEx[K: Packer](indexName: String, keyGetter: (V, DBSessionQueryable) => Set[K]): UniqueIndex[K, V] = {
      type FullKey = (Int, K)
      newIndex(indexName) {
        def v2fk(indexID: Int, e: V, db: DBSessionQueryable) = {
          keyGetter(e, db).map(k => Tuple2(indexID, k)).toSet
        }
        new AbstractIndexImpl[FullKey, V](indexName, this, unique = true, v2fk) with UniqueIndex[K, V] {

          override final def apply(key: K)(implicit db: DBSessionQueryable): Option[V] = {
            db.get[FullKey, Long](this.getIndexID, key).flatMap(db.get[Long, V])
          }

          final def contains(key: K)(implicit db: DBSessionQueryable): Boolean = {
            db.exists[FullKey](this.getIndexID, key)
          }

          override final def delete(key: K)(implicit db: DBSessionUpdatable): Unit = {
            db.get[FullKey, Long](this.getIndexID, key).foreach(host.delete)
          }

          override final def update(value: V)(implicit db: DBSessionUpdatable): Unit = {
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

    final def defIndex[K: Packer](indexName: String, keyGetter: V => K): Index[K, V] = {
      defIndexEx[K](indexName, (v, db) => Set.empty[K] + keyGetter(v))
    }

    final def defIndexEx[K: Packer](indexName: String, keyGetter: (V, DBSessionQueryable) => Set[K]): Index[K, V] = {
      type FullKey = (Int, K, Long)
      newIndex(indexName) {
        def v2fk(indexID: Int, e: V, db: DBSessionQueryable) = {
          keyGetter(e, db).map(k => Tuple3(indexID, k, e.id)).toSet
        }
        new AbstractIndexImpl[FullKey, V](indexName, this, unique = false, v2fk) with Index[K, V] {

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

  class SubHostCollectionImpl[O <: Entity, V <: Entity : Packer](name: String)
    extends HostCollectionImpl[SCE[O, V]](name) {
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

    def defUniqueIndex[K: Packer](owner: Ref[O], indexName: String, keyGetter: V => K): UniqueIndex[K, V] = {
      defUniqueIndexEx[K](owner, indexName, (v: V, db: DBSessionQueryable) => Set.empty[K] + keyGetter(v))
    }

    def defUniqueIndexEx[K: Packer](owner: Ref[O], indexName: String, keyGetter: (V, DBSessionQueryable) => Set[K]): UniqueIndex[K, V] = {
      type FullKey = (Int, Long, K)
      val rawIndex = getOrNewIndex(indexName) {
        def v2fk(indexID: Int, e: SCE[O, V], db: DBSessionQueryable) = {
          keyGetter(e.entity, db).map(k => Tuple3(indexID, e.owner.id, k)).toSet
        }
        new AbstractIndexImpl[FullKey, SCE[O, V]](indexName, this, unique = true, v2fk)
      }
      new UniqueIndex[K, V] {
        override def update(value: V)(implicit db: DBSessionUpdatable): Unit = {
          subHost.update(SCE(owner, value))
        }

        final def contains(key: K)(implicit db: DBSessionQueryable): Boolean = {
          db.exists[FullKey](rawIndex.getIndexID, owner.id, key)
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

    final def defIndex[K: Packer](owner: Ref[O], indexName: String, keyGetter: V => K): Index[K, V] = {
      defIndexEx[K](owner, indexName, (v: V, db: DBSessionQueryable) => Set.empty[K] + keyGetter(v))
    }

    final def defIndexEx[K: Packer](owner: Ref[O], indexName: String, keyGetter: (V, DBSessionQueryable) => Set[K]): Index[K, V] = {
      type FullKey = (Int, Long, K, Long)
      val rawIndex = getOrNewIndex(indexName) {
        def v2fk(indexID: Int, e: SCE[O, V], db: DBSessionQueryable) = {
          keyGetter(e.entity, db).map(k => Tuple4(indexID, e.owner.id, k, e.id)).toSet
        }
        new AbstractIndexImpl[FullKey, SCE[O, V]](indexName, this, unique = false, v2fk)
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

  class RefTracer(db: DBSessionQueryable, val rootEntityID: Long, refEntityID: Long, oldRefEntity: Entity, newRefEntity: Entity) extends DBSessionQueryable {
    var oldRefIDs = Set.empty[Long]
    var newRefIDs = Set.empty[Long]
    val oldRef = Option(oldRefEntity)
    val newRef = Option(newRefEntity)
    var asNew: Boolean = false

    @inline
    final def use(asNew: Boolean): Unit = {
      this.asNew = asNew
    }

    def get[K: Packer, V: Packer](key: K): Option[V] = key match {
      case id: Long =>
        if (id == refEntityID) {
          return (if (asNew) newRef else oldRef).asInstanceOf[Option[V]]
        }
        if (id != rootEntityID) {
          if (asNew) {
            newRefIDs += id
          } else {
            oldRefIDs += id
          }
        }
        db.get[Long, V](id)
      case _ => db.get[K, V](key)
    }

    def exists[K: Packer](key: K): Boolean = db.exists(key)

    def newCursor(): DBCursor = db.newCursor()

    def getMeta(name: String)(implicit db: DBSessionQueryable) = db.getMeta(name)
  }


  type Extract[S, DK, DV] = (S, DBSessionQueryable) => scala.collection.Map[DK, DV]
  type Extract2[K, V, DK, DV] = (K, V, DBSessionQueryable) => scala.collection.Map[DK, DV]

  class DataViewExtractor[K, V, K2, V2](val from: DataViewImpl[K, V], to: DataViewImpl[K2, V2], extract: Extract2[K, V, K2, V2]) {
    def update(k: K, oldV: Option[V], newV: Option[V], tracer: RefTracer)(implicit db: DBSessionUpdatable): Unit = {
      def ex(v: Option[V], asNew: Boolean) = {
        tracer.use(asNew)
        if (v.isEmpty) Map.empty.asInstanceOf[scala.collection.Map[K2, V2]] else extract(k, v.get, tracer)
      }
      to.updateDB(ex(oldV, asNew = false), ex(oldV, asNew = true), tracer)
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
    def update(rootEntityID: Long, oldRootEntity: E, newRootEntity: E)(implicit db: DBSessionUpdatable): Unit = {
      val tracer = new RefTracer(db, rootEntityID, rootEntityID, oldRootEntity, newRootEntity)
      tracer.use(asNew = false)
      val oldKV = if (oldRootEntity ne null) extract(oldRootEntity, tracer) else Map.empty[DK, DV]
      tracer.use(asNew = true)
      val newKV = if (newRootEntity ne null) extract(newRootEntity, tracer) else Map.empty[DK, DV]
      dv.updateDB(oldKV, newKV, tracer)
      ExtractorReverseIndex.update(dv.getDataViewID, tracer)
    }

    def update(rootEntityID: Long, refEntityID: Long, oldEntity: Entity, newEntity: Entity)(implicit db: DBSessionUpdatable): Unit = {
      val rootEntity = host.get1(rootEntityID)
      val tracer = new RefTracer(db, rootEntityID, refEntityID, oldEntity, newEntity)
      tracer.use(asNew = false)
      val oldKV = extract(rootEntity, tracer)
      tracer.use(asNew = true)
      val newKV = extract(rootEntity, tracer)
      dv.updateDB(oldKV, newKV, tracer)
      ExtractorReverseIndex.update(dv.getDataViewID, tracer)
    }

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

  class DataViewImpl[DK: Packer, DV: Packer](val name: String, val merger: Merger[DV])
                                            (implicit val fullKeyPacker: Packer[(Int, DK)], val vPacker: Packer[DV]) extends DBObject {

    val entityExtractors = new AtomicReference[EntityExtractor[_, DK, DV]](null)

    def defExtractor[E <: Entity](collection: EntityCollection[E], f: Extract[E, DK, DV]): Unit = {
      collection match {
        case r: RootCollection[E] =>
          new EntityExtractor(this, r.impl, f)
        case s: SubCollection[_, E] =>
          val ex = (sce: SCE[_, E], db: DBSessionQueryable) => f(sce.entity, db)
          new EntityExtractor(this, s.host, ex)
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

    @volatile private var _meta: DBType.DataViewMeta = null

    final def dbBind()(implicit db: DBSessionQueryable): Unit = {
      if (_meta ne null) {
        return
      }
      synchronized {
        if (_meta ne null) {
          return
        }
        db.getMeta(name) match {
          case x: DBType.DataViewMeta => _meta = x
          case x => throw new IllegalStateException(s"expect dataview meta, but found $x")
        }
      }
      bindDBObj(_meta.prefix, this)
    }

    @inline private final def getMeta(implicit db: DBSessionQueryable): DBType.DataViewMeta = {
      if (_meta ne null) {
        return _meta
      }
      dbBind()
      _meta
    }

    @inline final def getDataViewID(implicit db: DBSessionQueryable): Int = getMeta.prefix

    final def updateDB(oldKV: collection.Map[DK, DV], newKV: collection.Map[DK, DV], tracer: RefTracer)
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

    @inline final def apply(key: DK)(implicit db: DBSessionQueryable): Option[KeyValue[DK, DV]] = {
      import Range._
      val v = db.get(getDataViewID -> key)(fullKeyPacker, vPacker)
      if (v.isEmpty) {
        None
      } else {
        Some(KeyValue(key, v.get))
      }
    }

    @inline final def apply[P: Packer](range: Range[P], forward: Boolean = true)(implicit db: DBSessionQueryable, canUse: TuplePrefixed[DK, P]): Cursor[KeyValue[DK, DV]] = {
      val dvID = getDataViewID
      val r = if (canUse.shouldTuple) range.map(r => (dvID, Tuple1(r))) else range.map(r => (dvID, r))
      scan(r, forward)
    }
  }

  class AbstractIndexImpl[FK, E <: Entity](val name: String, host: HostCollectionImpl[E], unique: Boolean,
                                           v2fk: (Int, E, DBSessionQueryable) => Set[FK])
                                          (implicit val fullKeyPacker: Packer[FK]) extends DBObject {
    private var _meta: DBType.IndexMeta = null


    def dbBind()(implicit db: DBSessionQueryable): Unit = {
      if (_meta ne null) {
        return
      }
      _meta = host.getMeta.indexes.find(_.name == name).orNull
      if (_meta eq null) {
        throw new IllegalStateException(s"index $name db bind failed")
      }
      bindDBObj(_meta.prefix, this)
    }

    val nextInHost = {
      var next = host.indexes.get()
      while (!host.indexes.compareAndSet(next, this)) {
        next = host.indexes.get()
      }
      next
    }

    @inline final def getIndexID(implicit db: DBSessionQueryable): Int = {
      if (_meta ne null) {
        return _meta.prefix
      }
      dbBind()
      _meta.prefix
    }

    def doUpdate(oldEntity: E, newEntity: E, tracer: RefTracer)(implicit db: DBSessionUpdatable): Unit = {
      val indexID = getIndexID
      val oldFK = if (oldEntity ne null) {
        tracer.use(asNew = false)
        v2fk(indexID, oldEntity, tracer)
      } else {
        tracer.use(asNew = true)
        for (k <- v2fk(indexID, newEntity, db)) {
          if (unique) {
            db.put(k, tracer.rootEntityID)
          } else {
            db.put(k, true)
          }
        }
        return
      }
      val newFK = if (newEntity ne null) {
        tracer.use(asNew = true)
        v2fk(indexID, newEntity, tracer)
      } else {
        tracer.use(asNew = false)
        for (k <- v2fk(indexID, oldEntity, db)) {
          db.del(k)
        }
        return
      }
      for (fk <- newFK -- oldFK) {
        if (unique) {
          db.put(fk, tracer.rootEntityID)
        } else {
          db.put(fk, true)
        }
      }
      for (fk <- oldFK -- newFK) {
        db.del(fk)
      }
      ExtractorReverseIndex.update(indexID, tracer)
    }

    @inline
    final def update[R <: Entity](rootEntityID: Long, refEntityID: Long, oldRefEntity: R, newRefEntity: R)(implicit db: DBSessionUpdatable): Unit = {
      val root = host.get1(rootEntityID)
      val tracer = new RefTracer(db, rootEntityID, refEntityID, oldRefEntity, newRefEntity)
      doUpdate(root, root, tracer)
    }

    @inline
    final def update(rootEntityID: Long, oldRootEntity: E, newRootEntity: E)(implicit db: DBSessionUpdatable): Unit = {
      val tracer = new RefTracer(db, rootEntityID, rootEntityID, oldRootEntity, newRootEntity)
      doUpdate(oldRootEntity, newRootEntity, tracer)
    }

    @inline
    final def deleteByFullKey(fullKey: FK)(implicit db: DBSessionUpdatable): Unit = {
      val id = db.get[FK, Long](fullKey)
      if (id.isDefined) {
        host.delete(id.get)
      }
    }

    @inline
    final def getByFullKey[V](fullKey: FK)(implicit db: DBSessionQueryable): Option[E] = {
      val id = db.get[FK, Long](fullKey)
      if (id.isDefined) host.get0(id.get) else None
    }
  }

  @packable
  case class RRIKey(prefix: Int, refID: Long, byID: Long)

  object RefReverseIndex {
    final val prefix = Int.MaxValue - 1
    implicit val fullKeyPacker = Packer.of[RRIKey]

    @inline final def update(entityID: Long, oldRefs: Set[Long], newRefs: Set[Long])(implicit db: DBSessionUpdatable): Unit = {
      for (r <- newRefs) {
        if (!oldRefs.contains(r)) {
          db.put(RRIKey(prefix, r, entityID), true)
        }
      }
      for (r <- oldRefs) {
        if (!newRefs.contains(r)) {
          db.del(RRIKey(prefix, r, entityID))
        }
      }
    }

    @inline final def scanRefID(id: Long, cidFrom: Int, cidTo: Int)(implicit db: DBSessionQueryable): Cursor[Long] = {
      import Range._
      new RawKVCursor[Long](RRIKey(prefix, id, entityIDOf(cidFrom, 0)) +-+ RRIKey(prefix, id, entityIDOf(cidTo, Long.MaxValue)), forward = true) {
        override def buildValue() = fullKeyPacker.unapply(rawKey).byID
      }
    }

    @inline final def scan[V <: Entity](id: Long, host: HostCollectionImpl[V])(implicit db: DBSessionQueryable): Cursor[V] = {
      val (minCid, maxCid) = if (host eq null) (0, Int.MaxValue) else (host.getCollectionID, host.getCollectionID)
      scanRefID(id, minCid, maxCid).map(id => getEntity[V](id))
    }
  }

  @packable
  case class FTSIKey(prefix: Int, word: String, entityID: Long)

  object FullTextSearchIndex {

    final val prefix = Int.MaxValue - 2

    implicit val fullKeyPacker = Packer.of[FTSIKey]

    @inline final def update(entityID: Long, oldTexts: Seq[String], newTexts: Seq[String])(implicit db: DBSessionUpdatable): Unit = {
      val oldWords = FullTextSearch.split(oldTexts: _*)
      val newWords = FullTextSearch.split(newTexts: _*)
      for (w <- newWords) {
        if (!oldWords.contains(w)) {
          db.put(FTSIKey(prefix, w, entityID), true)
        }
      }
      for (w <- oldWords) {
        if (!newWords.contains(w)) {
          db.del(FTSIKey(prefix, w, entityID))
        }
      }
    }


    @inline final def scan[V <: Entity](word: String, host: HostCollectionImpl[V])(implicit db: DBSessionQueryable): Cursor[V] = {
      import Range._
      val r = if (host eq null) {
        FTSIKey(prefix, word, 0l) +-+ FTSIKey(prefix, word, Long.MaxValue)
      } else {
        FTSIKey(prefix, word, host.fixID(0)) +-+ FTSIKey(prefix, word, host.fixID(Long.MaxValue))
      }
      new RawKVCursor[V](r, forward = true) {
        override def buildValue() = getEntity[V](fullKeyPacker.unapply(rawKey).entityID)
      }
    }
  }

  @packable
  case class ERIKey(prefix: Int, refID: Long, extractID: Int, rootEntityID: Long)

  object ExtractorReverseIndex {
    final val prefix = Int.MaxValue - 3
    implicit val fullKeyPacker = Packer.of[ERIKey]

    def hasReverseExtracts(collectionID: Int)(implicit db: DBSessionUpdatable): Boolean = {
      val k = ERIKey(prefix, entityIDOf(collectionID, 0), 0, 0L)
      val c = db.newCursor()
      try {
        c.seek(fullKeyPacker(k))
        c.isValid && Util.compareUInt8s(c.key(), fullKeyPacker(ERIKey(prefix, entityIDOf(collectionID, Long.MaxValue), 0, 0L))) < 0
      } finally {
        c.close()
      }
    }

    def update(extractID: Int, tracer: RefTracer)(implicit db: DBSessionUpdatable): Unit = {
      for (rid <- tracer.newRefIDs) {
        if (!tracer.oldRefIDs.contains(rid)) {
          findDBObject(collectionIDOf(rid)).asInstanceOf[HostCollectionImpl[_]].setHasReverseExtracts()
          db.put(ERIKey(prefix, rid, extractID, tracer.rootEntityID), true)
        }
      }
      for (rid <- tracer.oldRefIDs) {
        if (!tracer.newRefIDs.contains(rid)) {
          db.del(ERIKey(prefix, rid, extractID, tracer.rootEntityID))
        }
      }
    }

    @inline final def scan(refID: Long)(implicit db: DBSessionQueryable): Cursor[ERIKey] = {
      import Range._
      new RawKVCursor[ERIKey](ERIKey(prefix, refID, 0, 0L) +-- ERIKey(prefix, refID, Int.MaxValue, Long.MaxValue), forward = true) {
        override def buildValue() = fullKeyPacker.unapply(rawKey)
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