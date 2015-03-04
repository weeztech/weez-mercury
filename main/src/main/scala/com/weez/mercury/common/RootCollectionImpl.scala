package com.weez.mercury.common

import com.weez.mercury.macros.TuplePrefixed

import scala.collection.mutable
import EntityCollections._

private class RootCollectionImpl[V <: Entity](val name: String, fullTextSupport: Boolean)(implicit val valuePacker: Packer[V]) extends DBObject with ExtractorSource[V] {
  self =>

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
    var oldTexts = List.empty[String]
    var newTexts = List.empty[String]

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
      case s: String => if (fullTextSupport && s.length > 0) {
        if (isNew)
          newTexts ::= s
        else
          oldTexts ::= s
      }
      case _ =>
    }
    search(oldEntity, isNew = false)
    search(newEntity, isNew = true)
    if (fullTextSupport) {
      FullTextSearchIndex.update(entityID, oldTexts, newTexts)
    }
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

  final def defUniqueIndex[K: Packer](indexName: String, keyGetter: V => K): UniqueView[K, V] = {
    defUniqueIndexEx[K](indexName, (v, db) => Set.empty[K] + keyGetter(v))
  }

  final def defUniqueIndexEx[K: Packer](indexName: String, keyGetter: (V, DBSessionQueryable) => Set[K]): UniqueView[K, V] = {
    type FullKey = (Int, K)
    newIndex(indexName) {
      new AbstractIndexImpl[FullKey, V](indexName, this, unique = true) with UniqueView[K, V] {

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

        def scan(dbRange: DBRange, forward: Boolean)(implicit db: DBSessionQueryable) = {
          new RawKVCursor[KeyValue[K, V]](dbRange, forward) {
            override def buildValue(): KeyValue[K, V] = {
              KeyValue[K, V](fullKeyPacker.unapply(rawKey)._2, self.get1(Packer.of[Long].unapply(rawValue)))
            }
          }
        }

        def apply()(implicit db: DBSessionQueryable): Cursor[KeyValue[K, V]] = {
          import Range._
          scan(Tuple1(getIndexID).asRange, forward = true)
        }

        def apply[P: Packer](range: Range[P], forward: Boolean)(implicit db: DBSessionQueryable, canUse: TuplePrefixed[K, P]): Cursor[KeyValue[K, V]] = {
          val r = if (canUse.shouldTuple)
            range.map(r => (getIndexID, Tuple1(r)))
          else
            range.map(r => (getIndexID, r))
          scan(r, forward)
        }
      }
    }
  }

  final def defIndex[K: Packer](indexName: String, keyGetter: V => K): View[K, V] = {
    defIndexEx[K](indexName, (v, db) => Set.empty[K] + keyGetter(v))
  }

  final def defIndexEx[K: Packer](indexName: String, keyGetter: (V, DBSessionQueryable) => Set[K]): View[K, V] = {
    type FullKey = (Int, K, Long)
    newIndex(indexName) {
      new AbstractIndexImpl[FullKey, V](indexName, this, unique = false) with View[K, V] {

        override def extract(s: V, tracer: RefTracer): Set[(Int, K, Long)] = {
          val indexID = getIndexID(tracer)
          keyGetter(s, tracer).map(k => Tuple3(indexID, k, s.id)).toSet
        }

        def scan(dbRange: DBRange, forward: Boolean)(implicit db: DBSessionQueryable) = {
          new RawKVCursor[KeyValue[K, V]](dbRange, forward) {
            override def buildValue(): KeyValue[K, V] = {
              val fk = fullKeyPacker.unapply(rawKey)
              KeyValue(fk._2, self.get1(fk._3))
            }
          }
        }

        def apply()(implicit db: DBSessionQueryable): Cursor[KeyValue[K, V]] = {
          import Range._
          scan(Tuple1(getIndexID).asRange, forward = true)
        }

        def apply[P: Packer](range: Range[P], forward: Boolean)(implicit db: DBSessionQueryable, canUse: TuplePrefixed[K, P]): Cursor[KeyValue[K, V]] = {
          val r = if (canUse.shouldTuple)
            range.map(r => (getIndexID, Tuple1(r)))
          else
            range.map(r => (getIndexID, r))
          scan(r, forward)
        }
      }
    }
  }
}

private object RootCollectionImpl {
  def apply[V <: Entity : Packer](stub: AnyRef): RootCollectionImpl[V] = {
    newDBObject(stub) { name =>
      new RootCollectionImpl[V](name, stub.isInstanceOf[FullTextSupport])
    }
  }
}

private abstract class AbstractIndexImpl[FK, E <: Entity](val name: String, host: RootCollectionImpl[E], unique: Boolean)
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
  final def containsByFullKey[V](fullKey: FK)(implicit db: DBSessionQueryable): Boolean = {
    db.exists[FK](fullKey)
  }


  @inline
  final def getByFullKey[V](fullKey: FK)(implicit db: DBSessionQueryable): Option[E] = {
    val id = db.get[FK, Long](fullKey)
    if (id.isEmpty) None else host.get0(id.get)
  }
}

final class Entity2DataViewExtractor[E <: Entity, K, V](source: RootCollectionImpl[E],
                                                        target: DataViewImpl[K, V],
                                                        f: (E, DBSessionQueryable) => Map[K, V])
  extends Extractor[E, Map[K, V]](source, target) {
  override def extract(s: E, tracer: RefTracer): Map[K, V] = f(s, tracer)
}

final class Entity2DataBoardExtractor[E <: Entity, D](source: RootCollectionImpl[E],
                                                      target: DataBoardImpl[D],
                                                      f: (E, DBSessionQueryable) => Seq[D])
  extends Extractor[E, Seq[D]](source, target) {
  override def extract(s: E, tracer: RefTracer): Seq[D] = f(s, tracer)
}
