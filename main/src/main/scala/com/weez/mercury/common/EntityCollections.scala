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

  def getDBObjectName(c: AnyRef): String = {
    val sn = c.getClass.getSimpleName
    var i = sn.length - 1
    while (sn.charAt(i) == '$') {
      i -= 1
    }
    Util.camelCase2seqStyle(sn.substring(0, i + 1))
  }

  def forSubCollection[O <: Entity, V <: Entity : Packer](sc: SubCollection[O, V]): SubHostCollectionImpl[O, V] = {
    val name = getDBObjectName(sc)
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

  def newDBObject[O <: DBObject](stub: AnyRef)(f: String => O): O = {
    val name = getDBObjectName(stub)
    dbObjectsByName synchronized {
      val exist = dbObjectsByName.get(name)
      if (exist.isDefined) {
        throw new IllegalArgumentException( s"""DB Object(${exist.get}) naming "$name" already exists!""")
      }
      val o = f(name)
      dbObjectsByName.put(name, o)
      o
    }
  }

  def newHost[V <: Entity : Packer](stub: AnyRef): HostCollectionImpl[V] = {
    newDBObject(stub) { name =>
      new HostCollectionImpl[V](name)
    }
  }

  @packable
  case class DataViewKey[K](prefix: Int, key: K)

  import scala.reflect.runtime.universe._

  trait CC extends CanMerge[CC]

  def newDataView[K: Packer, V: TypeTag](stub: AnyRef)(implicit vPacker: Packer[V]): DataViewImpl[K, V] = {
    val fkp = Packer.of[DataViewKey[K]]
    def newMDV(name: String, noDBMerge: Boolean) = {
      new MergeDataViewImpl[K, CC](name, noDBMerge)(fkp, vPacker.asInstanceOf[Packer[CC]]).asInstanceOf[DataViewImpl[K, V]]
    }
    newDBObject(stub) { name =>
      if (typeOf[V] <:< typeOf[CanMerge[_]]) {
        newMDV(name, stub.isInstanceOf[NotMergeInDB])
      } else {
        new DataViewImpl[K, V](name)(fkp, vPacker)
      }
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

  class HostCollectionImpl[V <: Entity](val name: String)(implicit val valuePacker: Packer[V]) extends DBObject with ExtractorSource[V] {
    host =>

    @volatile private var _meta: DBType.CollectionMeta = null
    private val listeners = mutable.AnyRefMap[EntityCollectionListener[_], ListenerInfo]()
    @volatile private var deleteListeners = emptyListeners[V]
    @volatile private var updateListeners = emptyListeners[V]
    @volatile private var insertListeners = emptyListeners[V]

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
      foreachSourceExtractor(_.target.dbBind())
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

    private var hasReverseExtracts = 0

    @inline final def setHasReverseExtracts() = hasReverseExtracts = 1

    private final def updateInternals(entityID: Long, oldEntity: V, newEntity: V)(implicit db: DBSessionUpdatable): Unit = {
      //全文检索和反向索引
      var oldRefs = Set.empty[Long]
      var newRefs = Set.empty[Long]
      //var oldTexts = List.empty[String]
      //var newTexts = List.empty[String]

      def search(e: Any, isNew: Boolean): Unit = e match {
        case p: Iterable[_] =>
          p.foreach(e => search(e, isNew))
        case r: Ref[_] =>
          if (r.isDefined) {
            if (isNew)
              newRefs += r.id
            else
              oldRefs += r.id
          }
        case p: Product =>
          var i = p.productArity - 1
          while (i >= 0) {
            search(p.productElement(i), isNew)
            i -= 1
          }
        //        case s: String => if (s.length > 0) {
        //          if (isNew)
        //            newTexts ::= s
        //          else
        //            oldTexts ::= s
        //}
        case _ =>
      }
      search(oldEntity, isNew = false)
      search(newEntity, isNew = true)
      //FullTextSearchIndex.update(entityID, oldTexts, newTexts)
      RefReverseIndex.update(entityID, oldRefs, newRefs)
      //extractor
      if (newEntity eq null) {
        val tracer = new RefTracer(db, entityID, entityID, Some(oldEntity), None)
        foreachSourceExtractor(_.doExtractOld(oldEntity, tracer))
      } else if (oldEntity eq null) {
        val tracer = new RefTracer(db, entityID, entityID, None, Some(newEntity))
        foreachSourceExtractor(_.doExtractNew(newEntity, tracer))
      } else {
        val tracer = new RefTracer(db, entityID, entityID, Some(oldEntity), Some(newEntity))
        foreachSourceExtractor(_.doExtractBoth(oldEntity, newEntity, tracer))
        //间接 extractor
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
            var rid = 0l
            for (x <- cursor) {
              if (rid != x.rootEntityID) {
                rid = x.rootEntityID
                tracer.rootEntityID = rid
              }
              findDBObject(x.extractID) match {
                case et: IndirectExtract =>
                  et.indirectExtract(tracer)
                case _ => throw new IllegalStateException(s"extractor missing! id: ${x.extractID}")
              }
            }
          } finally {
            cursor.close()
          }
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
      var idx: AbstractIndexImpl[_, V] = null
      foreachSourceExtractor {
        case x: AbstractIndexImpl[_, V] =>
          if (x.name == indexName) idx = x
        case _ =>
      }
      idx
    }

    final def newIndex[INDEX <: AbstractIndexImpl[_, V]](indexName: String)(f: => INDEX): INDEX = {
      synchronized {
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
        new AbstractIndexImpl[FullKey, V](indexName, this, unique = true) with UniqueIndex[K, V] {

          override def extract(s: V, tracer: RefTracer): Set[(Int, K)] = {
            val indexID = getIndexID(tracer)
            keyGetter(s, tracer).map(k => Tuple2(indexID, k)).toSet
          }

          override final def apply(key: K)(implicit db: DBSessionQueryable): Option[V] = {
            getByFullKey(this.getIndexID -> key)
          }

          final def contains(key: K)(implicit db: DBSessionQueryable): Boolean = {
            containsByFullKey(this.getIndexID -> key)
          }

          override final def delete(key: K)(implicit db: DBSessionUpdatable): Unit = {
            deleteByFullKey(this.getIndexID -> key)
          }

          override final def update(value: V)(implicit db: DBSessionUpdatable): Unit = {
            host.update(value)
          }

          def apply()(implicit db: DBSessionQueryable): Cursor[V] = {
            import Range._
            new RawKVCursor[V](Tuple1(getIndexID).asRange, forward = false) {
              override def buildValue(): V = {
                host.get1(Packer.of[Long].unapply(rawValue))
              }
            }
          }

          def apply[P: Packer](range: Range[P], forward: Boolean)(implicit db: DBSessionQueryable, canUse: TuplePrefixed[K, P]): Cursor[V] = {
            val r = if (canUse.shouldTuple)
              range.map(r => (getIndexID, Tuple1(r)))
            else
              range.map(r => (getIndexID, r))
            new RawKVCursor[V](r, forward) {
              override def buildValue(): V = {
                host.get1(Packer.of[Long].unapply(rawValue))
              }
            }
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
        new AbstractIndexImpl[FullKey, V](indexName, this, unique = false) with Index[K, V] {

          override def extract(s: V, tracer: RefTracer): Set[(Int, K, Long)] = {
            val indexID = getIndexID(tracer)
            keyGetter(s, tracer).map(k => Tuple3(indexID, k, s.id)).toSet
          }

          def apply()(implicit db: DBSessionQueryable): Cursor[V] = {
            import Range._
            new RawKVCursor[V](Tuple1(getIndexID).asRange, forward = true) {
              override def buildValue(): V = {
                host.get1(fullKeyPacker.unapply(rawKey)._3)
              }
            }
          }

          def apply[P: Packer](range: Range[P], forward: Boolean)(implicit db: DBSessionQueryable, canUse: TuplePrefixed[K, P]): Cursor[V] = {
            val r = if (canUse.shouldTuple)
              range.map(r => (getIndexID, Tuple1(r)))
            else
              range.map(r => (getIndexID, r))
            new RawKVCursor[V](r, forward) {
              override def buildValue(): V = {
                host.get1(fullKeyPacker.unapply(rawKey)._3)
              }
            }
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
        new AbstractIndexImpl[FullKey, SCE[O, V]](indexName, this, unique = true) {
          override def extract(s: SCE[O, V], tracer: RefTracer): Set[(Int, Long, K)] = {
            val indexID = getIndexID(tracer)
            keyGetter(s.entity, tracer).map(k => Tuple3(indexID, s.owner.id, k)).toSet
          }
        }
      }
      new UniqueIndex[K, V] {
        override def update(value: V)(implicit db: DBSessionUpdatable): Unit = {
          subHost.update(SCE(owner, value))
        }

        final def contains(key: K)(implicit db: DBSessionQueryable): Boolean = {
          rawIndex.containsByFullKey((rawIndex.getIndexID, owner.id, key))
        }

        override def delete(key: K)(implicit db: DBSessionUpdatable): Unit = {
          rawIndex.deleteByFullKey((rawIndex.getIndexID, owner.id, key))
        }

        override def apply(key: K)(implicit db: DBSessionQueryable): Option[V] = {
          rawIndex.getByFullKey(rawIndex.getIndexID, owner.id, key).map(_.entity)
        }

        override def apply()(implicit db: DBSessionQueryable): Cursor[V] = {
          import Range._
          new RawKVCursor[V]((rawIndex.getIndexID, owner.id).asRange, forward = true) {
            override def buildValue(): V = {
              get1(Packer.of[Long].unapply(rawValue)).entity
            }
          }
        }

        override def apply[P: Packer](range: Range[P], forward: Boolean)(implicit db: DBSessionQueryable, canUse: TuplePrefixed[K, P]): Cursor[V] = {
          val r = if (canUse.shouldTuple)
            range.map(r => (rawIndex.getIndexID, owner.id, Tuple1(r)))
          else
            range.map(r => (rawIndex.getIndexID, owner.id, r))
          new RawKVCursor[V](r, forward) {
            override def buildValue(): V = {
              get1(Packer.of[Long].unapply(rawValue)).entity
            }
          }
        }
      }
    }

    final def defIndex[K: Packer](owner: Ref[O], indexName: String, keyGetter: V => K): Index[K, V] = {
      defIndexEx[K](owner, indexName, (v: V, db: DBSessionQueryable) => Set.empty[K] + keyGetter(v))
    }

    final def defIndexEx[K: Packer](owner: Ref[O], indexName: String, keyGetter: (V, DBSessionQueryable) => Set[K]): Index[K, V] = {
      type FullKey = (Int, Long, K, Long)
      val rawIndex = getOrNewIndex(indexName) {
        new AbstractIndexImpl[FullKey, SCE[O, V]](indexName, this, unique = false) {
          override def extract(s: SCE[O, V], tracer: RefTracer): Set[(Int, Long, K, Long)] = {
            val indexID = getIndexID(tracer)
            keyGetter(s.entity, tracer).map(k => Tuple4(indexID, s.owner.id, k, s.id)).toSet
          }
        }
      }
      new Index[K, V] {
        override def apply()(implicit db: DBSessionQueryable): Cursor[V] = {
          import Range._
          new RawKVCursor[V]((rawIndex.getIndexID, owner.id).asRange, forward = true) {
            override def buildValue(): V = {
              get1(rawIndex.fullKeyPacker.unapply(rawKey)._4).entity
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
              get1(rawIndex.fullKeyPacker.unapply(rawKey)._4).entity
            }
          }
        }
      }
    }
  }

  class RefTracer(db: DBSessionQueryable,
                  var rootEntityID: Long,
                  refEntityID: Long,
                  oldRef: Option[Entity],
                  newRef: Option[Entity]) extends DBSessionQueryable {
    var oldRefIDs = Set.empty[Long]
    var newRefIDs = Set.empty[Long]

    @inline
    final def reset(): Unit = {
      oldRefIDs = Set.empty[Long]
      newRefIDs = Set.empty[Long]
    }

    private var asNew: Boolean = false

    @inline
    final def useNew() = {
      this.asNew = true
      this
    }

    @inline
    final def useOld() = {
      this.asNew = false
      this
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

  trait ExtractorSource[S] {
    val sourceExtractors = new AtomicReference[Extractor[S, _]]

    @inline
    final def foreachSourceExtractor[U](f: Extractor[S, _] => U): Unit = {
      var ext = sourceExtractors.get
      while (ext ne null) {
        f(ext)
        ext = ext.nextInSource
      }
    }
  }

  trait ExtractorTarget[T] {
    def targetExtractBoth(oldT: T, newT: T, tracer: RefTracer)(implicit db: DBSessionUpdatable): Unit

    def targetExtractOld(oldT: T, tracer: RefTracer)(implicit db: DBSessionUpdatable): Unit

    def targetExtractNew(newT: T, tracer: RefTracer)(implicit db: DBSessionUpdatable): Unit

    val targetExtractors = new AtomicReference[Extractor[_, T]]

    def dbBind()(implicit db: DBSessionQueryable): Unit = {}

    @inline
    final def foreachTargetExtractor[U](f: Extractor[_, T] => U): Unit = {
      var ext = targetExtractors.get
      while (ext ne null) {
        f(ext)
        ext = ext.nextInTarget
      }
    }

    def findExtractor[S](source: ExtractorSource[S]): Extractor[S, T] = {
      var e = targetExtractors.get()
      while (e ne null) {
        if (e.source eq source) {
          return e.asInstanceOf[Extractor[S, T]]
        }
        e = e.nextInTarget
      }
      null
    }
  }

  trait IndirectExtract {
    def indirectExtract(refTracer: RefTracer)(implicit db: DBSessionUpdatable): Unit
  }

  abstract class Extractor[S, T](val source: ExtractorSource[S], t: ExtractorTarget[T]) {
    val target: ExtractorTarget[T] = if (t eq null) this.asInstanceOf[ExtractorTarget[T]] else t

    if ((t ne null) && (t.findExtractor(source) ne null)) {
      throw new IllegalArgumentException("dump extractor")
    }

    @volatile var nextInSource = source.sourceExtractors.get
    while (!source.sourceExtractors.compareAndSet(nextInSource, this)) {
      nextInSource = source.sourceExtractors.get()
    }
    @volatile var nextInTarget = if (t eq null) null else t.targetExtractors.get
    while ((t ne null) && !t.targetExtractors.compareAndSet(nextInTarget, this)) {
      nextInTarget = t.targetExtractors.get()
    }

    def extract(s: S, tracer: RefTracer): T

    @inline
    final def doExtractBoth(oldS: S, newS: S, tracer: RefTracer)(implicit db: DBSessionUpdatable): Unit = {
      target.targetExtractBoth(extract(oldS, tracer.useOld()), extract(newS, tracer.useNew()), tracer)
    }

    @inline
    final def doExtractBoth(oldS: S, newS: S, tracer: RefTracer, next: Extractor[T, _])(implicit db: DBSessionUpdatable): Unit = {
      next.doExtractBoth(extract(oldS, tracer.useOld()), extract(newS, tracer.useNew()), tracer)
    }

    @inline
    final def doExtractNew(newS: S, tracer: RefTracer)(implicit db: DBSessionUpdatable): Unit = {
      val newT = extract(newS, tracer.useNew())
      target.targetExtractNew(newT, tracer)
    }

    @inline
    final def doExtractOld(oldS: S, tracer: RefTracer)(implicit db: DBSessionUpdatable): Unit = {
      target.targetExtractOld(extract(oldS, tracer.useOld()), tracer)
    }
  }


  class Entity2DataViewExtractor[E <: Entity, K, V](source: HostCollectionImpl[E],
                                                    target: DataViewImpl[K, V],
                                                    f: (E, DBSessionQueryable) => Map[K, V])
    extends Extractor[E, Map[K, V]](source, target) {
    override def extract(s: E, tracer: RefTracer): Map[K, V] = f(s, tracer)
  }

  def newEntity2DataViewExtractor[E <: Entity, K, V](collection: EntityCollection[E], dataView: DataViewImpl[K, V], f: (E, DBSessionQueryable) => Map[K, V]): Extractor[_, _] = {
    collection match {
      case r: RootCollection[E] =>
        new Entity2DataViewExtractor(r.impl, dataView, f)
      case s: SubCollection[_, E] =>
        val v = dataView.findExtractor(s.host)
        if (v ne null) {
          v
        } else {
          val ex = (sce: SCE[_, E], db: DBSessionQueryable) => f(sce.entity, db)
          new Entity2DataViewExtractor(s.host, dataView, ex)
        }
    }
  }

  class Entity2DataBoardExtractor[E <: Entity, D](source: HostCollectionImpl[E],
                                                  target: DataBoardImpl[D],
                                                  f: (E, DBSessionQueryable) => Seq[D])
    extends Extractor[E, Seq[D]](source, target) {
    override def extract(s: E, tracer: RefTracer): Seq[D] = f(s, tracer)
  }

  def newEntity2DataBoardExtractor[E <: Entity, D](collection: EntityCollection[E], dataBoard: DataBoardImpl[D], f: (E, DBSessionQueryable) => Seq[D]): Extractor[_, _] = {
    collection match {
      case r: RootCollection[E] =>
        new Entity2DataBoardExtractor(r.impl, dataBoard, f)
      case s: SubCollection[_, E] =>
        val v = dataBoard.findExtractor(s.host)
        if (v ne null) {
          v
        } else {
          val ex = (sce: SCE[_, E], db: DBSessionQueryable) => f(sce.entity, db)
          new Entity2DataBoardExtractor(s.host, dataBoard, ex)
        }
    }
  }

  class DataBoard2DataViewExtractor[SD, TK, TV](source: DataBoardImpl[SD],
                                                target: DataViewImpl[TK, TV],
                                                f: (SD, DBSessionQueryable) => Seq[(TK, TV)])
    extends Extractor[Seq[SD], Map[TK, TV]](source, target) {
    override def extract(s: Seq[SD], tracer: RefTracer): Map[TK, TV] = {
      var result = Map.empty[TK, TV]
      for (d <- s; (k, v) <- f(d, tracer)) {
        val exist = result.get(k)
        if (exist.isEmpty) {
          result = result.updated(k, v)
        } else {
          result = result.updated(k, target.merge(exist.get, v))
        }
      }
      result
    }
  }

  import scala.reflect.runtime.universe._

  class DataViewImpl[DK, DV](val name: String)
                            (implicit fullKeyPacker: Packer[DataViewKey[DK]], vPacker: Packer[DV])
    extends DBObject with ExtractorTarget[Map[DK, DV]] with IndirectExtract {

    @inline final def put(fk: DataViewKey[DK], v: DV)(implicit db: DBSessionUpdatable): Unit = db.put(fk, v)

    @inline final def del(fk: DataViewKey[DK])(implicit db: DBSessionUpdatable): Unit = db.del(fk)

    @inline final def get(fk: DataViewKey[DK])(implicit db: DBSessionQueryable): Option[DV] = db.get(fk)(fullKeyPacker, vPacker)

    @inline final def put(k: DK, v: DV)(implicit db: DBSessionUpdatable): Unit = put(DataViewKey(getDataViewID, k), v)

    @inline final def del(k: DK)(implicit db: DBSessionUpdatable): Unit = del(DataViewKey(getDataViewID, k))

    @inline final def get(k: DK)(implicit db: DBSessionQueryable): Option[DV] = get(DataViewKey(getDataViewID, k))

    def targetExtractBoth(oldKV: Map[DK, DV], newKV: Map[DK, DV], tracer: RefTracer)(implicit db: DBSessionUpdatable): Unit = {
      for ((k, v) <- newKV) {
        val ov = oldKV.get(k)
        if (ov.isEmpty || ov.get != v) {
          put(k, v)
        }
      }
      for ((k, v) <- oldKV) {
        if (!newKV.contains(k)) {
          del(k)
        }
      }
      ExtractorReverseIndex.update(getDataViewID, tracer)
    }

    def targetExtractOld(oldKV: Map[DK, DV], tracer: RefTracer)(implicit db: DBSessionUpdatable): Unit = {
      for ((k, _) <- oldKV) {
        del(k)
      }
      ExtractorReverseIndex.update(getDataViewID, tracer)
    }

    def targetExtractNew(newKV: Map[DK, DV], tracer: RefTracer)(implicit db: DBSessionUpdatable): Unit = {
      for ((k, v) <- newKV) {
        put(k, v)
      }
      ExtractorReverseIndex.update(getDataViewID, tracer)
    }


    def indirectExtract(tracer: RefTracer)(implicit db: DBSessionUpdatable): Unit = {
      val rid = tracer.rootEntityID
      val host = getHost[Entity](collectionIDOf(rid))
      var e = targetExtractors.get
      while (e ne null) {
        e.source match {
          case `host` =>
            val root = host.get1(rid)
            e.asInstanceOf[Extractor[Entity, Map[DK, DV]]].doExtractBoth(root, root, tracer)
            e = null
          case b: DataBoardImpl[_] =>
            if (b.indirectExtract(tracer, host, e)) {
              e = null
            }
          case _ =>
            e = e.nextInTarget
        }
      }
    }

    @volatile private var _meta: DBType.DataViewMeta = null

    override final def dbBind()(implicit db: DBSessionQueryable): Unit = {
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


    def merge(v1: DV, v2: DV) = v2

    @inline final def getDataViewID(implicit db: DBSessionQueryable): Int = getMeta.prefix

    final def scan(dbRange: DBRange, forward: Boolean = true)(implicit db: DBSessionQueryable): Cursor[KeyValue[DK, DV]] = {
      new RawKVCursor[KeyValue[DK, DV]](dbRange, forward) {
        override def buildValue() = {
          val rk = fullKeyPacker.unapply(rawKey)
          val rv = vPacker.unapply(rawValue)
          KeyValue[DK, DV](rk.key, rv)
        }
      }
    }

    final def apply(forward: Boolean)(implicit db: DBSessionQueryable): Cursor[KeyValue[DK, DV]] = {
      import Range._
      val prefix = Tuple1(getDataViewID)
      scan(prefix +-+ prefix, forward)
    }

    final def apply(key: DK)(implicit db: DBSessionQueryable): Option[KeyValue[DK, DV]] = {
      get(key).map(KeyValue(key, _))
    }

    final def contains(key: DK)(implicit db: DBSessionQueryable): Boolean = db.exists(DataViewKey(getDataViewID, key))

    final def apply[P: Packer](range: Range[P], forward: Boolean = true)(implicit db: DBSessionQueryable, canUse: TuplePrefixed[DK, P]): Cursor[KeyValue[DK, DV]] = {
      val dvID = getDataViewID
      val r = if (canUse.shouldTuple) range.map(r => (dvID, Tuple1(r))) else range.map(r => (dvID, r))
      scan(r, forward)
    }
  }

  class MergeDataViewImpl[DK, DV <: CanMerge[DV]](name: String, noDBMerge: Boolean)(implicit fullKeyPacker: Packer[DataViewKey[DK]], vPacker: Packer[DV])
    extends DataViewImpl[DK, DV](name) {

    override def merge(v1: DV, v2: DV): DV = v1 + v2

    @inline
    private final def putOld(fk: DataViewKey[DK], v: DV)(implicit db: DBSessionUpdatable): Unit = {
      if (v.isEmpty) {
        return
      }
      val inDB = get(fk)
      if (inDB.isEmpty) {
        val vv = v.neg()
        if (!vv.isEmpty) {
          put(fk, vv)
        }
      } else {
        val merged = inDB.get - v
        if (merged.isEmpty) {
          del(fk)
        } else {
          put(fk, merged)
        }
      }
    }

    @inline
    private final def putNew(fk: DataViewKey[DK], v: DV)(implicit db: DBSessionUpdatable): Unit = {
      if (v.isEmpty) {
        return
      }
      val inDB = get(fk)
      if (inDB.isEmpty) {
        put(fk, v)
      } else {
        val merged = inDB.get + v
        if (merged.isEmpty) {
          del(fk)
        } else {
          put(fk, merged)
        }
      }
    }

    override def targetExtractOld(oldKV: Map[DK, DV], tracer: RefTracer)(implicit db: DBSessionUpdatable): Unit = {
      if (noDBMerge) {
        super.targetExtractOld(oldKV, tracer)
      } else {
        val viewID = getDataViewID
        for ((k, v) <- oldKV) {
          putOld(DataViewKey(viewID, k), v)
        }
      }
    }

    override def targetExtractNew(newKV: Map[DK, DV], tracer: RefTracer)(implicit db: DBSessionUpdatable): Unit = {
      if (noDBMerge) {
        super.targetExtractNew(newKV, tracer)
      } else {
        val viewID = getDataViewID
        for ((k, v) <- newKV) {
          putNew(DataViewKey(viewID, k), v)
        }
      }
    }

    override def targetExtractBoth(oldKV: Map[DK, DV], newKV: Map[DK, DV], tracer: RefTracer)(implicit db: DBSessionUpdatable): Unit = {
      if (noDBMerge) {
        super.targetExtractBoth(oldKV, newKV, tracer)
      } else {
        val viewID = getDataViewID
        for ((k, v) <- newKV) {
          val fk = DataViewKey(viewID, k)
          val ov = oldKV.get(k)
          if (ov.isEmpty) {
            putNew(fk, v)
          } else if (v != ov.get) {
            val inDB = get(fk)
            if (inDB.isEmpty) {
              put(fk, v - ov.get)
            } else {
              val dmv = inDB.get - ov.get + v
              if (dmv.isEmpty) {
                del(fk)
              } else {
                put(fk, dmv)
              }
            }
          }
        }
        for ((k, v) <- oldKV) {
          if (!newKV.contains(k)) {
            putOld(DataViewKey(viewID, k), v)
          }
        }
      }
    }
  }

  final class DataBoardImpl[D] extends ExtractorSource[Seq[D]] with ExtractorTarget[Seq[D]] {
    def indirectExtract(tracer: RefTracer, host: HostCollectionImpl[Entity], next: Extractor[_, _])(implicit db: DBSessionUpdatable): Boolean = {
      var e = targetExtractors.get
      while (e ne null) {
        if (e.source == host) {
          val root = host.get1(tracer.rootEntityID)
          e.asInstanceOf[Extractor[Entity, Seq[D]]].doExtractBoth(root, root, tracer, next.asInstanceOf[Extractor[Seq[D], _]])
          return true
        } else {
          e = e.nextInTarget
        }
      }
      false
    }

    override def targetExtractBoth(oldT: Seq[D], newT: Seq[D], tracer: RefTracer)(implicit db: DBSessionUpdatable): Unit = {
      val oldRefIDs = tracer.oldRefIDs
      val newRefIDs = tracer.newRefIDs
      foreachSourceExtractor { se =>
        tracer.oldRefIDs = oldRefIDs
        tracer.newRefIDs = newRefIDs
        se.doExtractBoth(oldT, newT, tracer)
      }
    }

    override def targetExtractOld(oldT: Seq[D], tracer: RefTracer)(implicit db: DBSessionUpdatable): Unit = {
      val oldRefIDs = tracer.oldRefIDs
      foreachSourceExtractor { se =>
        tracer.oldRefIDs = oldRefIDs
        se.doExtractOld(oldT, tracer)
      }
    }

    override def targetExtractNew(newT: Seq[D], tracer: RefTracer)(implicit db: DBSessionUpdatable): Unit = {
      val newRefIDs = tracer.newRefIDs
      foreachSourceExtractor { se =>
        tracer.newRefIDs = newRefIDs
        se.doExtractNew(newT, tracer)
      }
    }
  }

  abstract class AbstractIndexImpl[FK, E <: Entity](val name: String, host: HostCollectionImpl[E], unique: Boolean)
                                                   (implicit val fullKeyPacker: Packer[FK])
    extends Extractor[E, Set[FK]](host, null) with DBObject with ExtractorTarget[Set[FK]] with IndirectExtract {

    def targetExtractBoth(oldFK: Set[FK], newFK: Set[FK], tracer: RefTracer)(implicit db: DBSessionUpdatable): Unit = {
      for (fk <- newFK) {
        if (!oldFK.contains(fk)) {
          if (unique) {
            db.put(fk, tracer.rootEntityID)
          } else {
            db.put(fk, true)
          }
        }
      }
      for (fk <- oldFK) {
        if (!newFK.contains(fk)) {
          db.del(fk)
        }
      }
      ExtractorReverseIndex.update(getIndexID, tracer)
    }

    def targetExtractOld(oldFK: Set[FK], tracer: RefTracer)(implicit db: DBSessionUpdatable): Unit = {
      for (fk <- oldFK) {
        db.del(fk)
      }
      ExtractorReverseIndex.update(getIndexID, tracer)
    }

    def targetExtractNew(newFK: Set[FK], tracer: RefTracer)(implicit db: DBSessionUpdatable): Unit = {
      for (fk <- newFK) {
        if (unique) {
          db.put(fk, tracer.rootEntityID)
        } else {
          db.put(fk, true)
        }
      }
      ExtractorReverseIndex.update(getIndexID, tracer)
    }


    def indirectExtract(tracer: RefTracer)(implicit db: DBSessionUpdatable): Unit = {
      val root = host.get1(tracer.rootEntityID)
      doExtractBoth(root, root, tracer)
    }

    private var _meta: DBType.IndexMeta = null


    override def dbBind()(implicit db: DBSessionQueryable): Unit = {
      if (_meta ne null) {
        return
      }
      _meta = host.getMeta.indexes.find(_.name == name).orNull
      if (_meta eq null) {
        throw new IllegalStateException(s"index $name db bind failed")
      }
      bindDBObj(_meta.prefix, this)
    }

    @inline final def getIndexID(implicit db: DBSessionQueryable): Int = {
      if (_meta ne null) {
        return _meta.prefix
      }
      dbBind()
      _meta.prefix
    }

    @inline
    final def deleteByFullKey(fullKey: FK)(implicit db: DBSessionUpdatable): Unit = {
      val id = db.get[FK, Long](fullKey)
      if (id.isDefined) {
        host.delete(id.get)
      }
    }

    @inline
    final def containsByFullKey[V](fullKey: FK)(implicit db: DBSessionQueryable): Boolean = {
      db.exists[FK](fullKey)
    }


    @inline
    final def getByFullKey[V](fullKey: FK)(implicit db: DBSessionQueryable): Option[E] = {
      val id = db.get[FK, Long](fullKey)
      if (id.isEmpty) None else host.get0(id.get)
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
      return
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
  case class ERIKey(prefix: Int, refID: Long, rootEntityID: Long, extractID: Int)

  object ExtractorReverseIndex {
    final val prefix = Int.MaxValue - 3
    implicit val fullKeyPacker = Packer.of[ERIKey]

    def hasReverseExtracts(collectionID: Int)(implicit db: DBSessionUpdatable): Boolean = {
      val k = ERIKey(prefix, entityIDOf(collectionID, 0), 0L, 0)
      val c = db.newCursor()
      try {
        c.seek(fullKeyPacker(k))
        c.isValid && Util.compareUInt8s(c.key(), fullKeyPacker(ERIKey(prefix, entityIDOf(collectionID, Long.MaxValue), 0, 0))) < 0
      } finally {
        c.close()
      }
    }

    def update(extractID: Int, tracer: RefTracer)(implicit db: DBSessionUpdatable): Unit = {
      for (rid <- tracer.newRefIDs) {
        if (!tracer.oldRefIDs.contains(rid)) {
          findDBObject(collectionIDOf(rid)).asInstanceOf[HostCollectionImpl[_]].setHasReverseExtracts()
          db.put(ERIKey(prefix, rid, tracer.rootEntityID, extractID), true)
        }
      }
      for (rid <- tracer.oldRefIDs) {
        if (!tracer.newRefIDs.contains(rid)) {
          db.del(ERIKey(prefix, rid, tracer.rootEntityID, extractID))
        }
      }
      tracer.reset()
    }

    @inline final def scan(refID: Long)(implicit db: DBSessionQueryable): Cursor[ERIKey] = {
      import Range._
      new RawKVCursor[ERIKey](ERIKey(prefix, refID, 0L, 0) +-- ERIKey(prefix, refID, Long.MaxValue, Int.MaxValue), forward = true) {
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
        if (!valid) {
          close()
        }
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