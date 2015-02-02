package com.weez.mercury.common

import java.util.concurrent.atomic.AtomicReference

import com.huaban.analysis.jieba.JiebaSegmenter
import com.weez.mercury.common.DBType.IndexMeta

import scala.collection.immutable
import scala.reflect.runtime.universe._

private object EntityCollections {


  val emptyStringSeq = Seq[String]()

  @inline final def collectionIDOf(entityID: Long): Int = (entityID >>> 48).asInstanceOf[Int]

  @inline final def entityIDOf(collectionID: Int, rawID: Long): Long = (rawID & 0xFFFFFFFFFFFFL) | (collectionID.asInstanceOf[Long] << 48)

  @inline final def getEntity[V <: Entity](id: Long)(implicit db: DBSessionQueryable): V = {
    this.getHost(collectionIDOf(id)).get1(id).asInstanceOf[V]
  }

  def getHost(collectionID: Int)(implicit db: DBSessionQueryable): HostCollectionImpl[_] = {
    val host = hostsByID(collectionID)
    if (host ne null) {
      return host
    }
    hosts.synchronized {
      if (hosts.size != bondHostCount) {
        for (h <- hosts.values) {
          h.getMeta
        }
      }
    }
    val host2 = hostsByID(collectionID)
    if (host2 eq null) {
      None.get
    } else {
      host2
    }
  }

  def forSubCollection[O <: Entity, V <: Entity : Packer](pc: SubCollection[O, V]): SubHostCollectionImpl[O, V] = {
    val name = pc.name
    hosts.synchronized {
      hosts.get(name) match {
        case Some(host: SubHostCollectionImpl[O, V]) => host
        case None =>
          val fxFunc = pc.fxFunc match {
            case f: (V => Seq[String]) => (sce: SCE[O, V]) => f(sce.entity)
            case _ => null
          }
          val host = new SubHostCollectionImpl[O, V](name, fxFunc)
          hosts.put(name, host)
          host
        case _ =>
          throw new IllegalArgumentException( s"""SubCollection name conflict :$name""")
      }
    }
  }

  def newHost[V <: Entity : Packer](name: String, getFX: V => Seq[String]): HostCollectionImpl[V] = {
    hosts.synchronized {
      if (hosts.contains(name)) {
        throw new IllegalArgumentException( s"""HostCollection naming "$name" exist!""")
      }
      val host = new HostCollectionImpl[V](name, getFX)
      hosts.put(name, host)
      host
    }
  }

  val hosts = collection.mutable.HashMap[String, HostCollectionImpl[_]]()
  val hostsByID = new Array[HostCollectionImpl[_]](0xFFFF)
  var bondHostCount = 0

  final val RefType = typeOf[Ref[Entity]]

  final val ProductType = typeOf[Product]

  val _emptyListeners = new Array[EntityCollectionListener[_]](0)

  def emptyListeners[V <: Entity] = _emptyListeners.asInstanceOf[Array[EntityCollectionListener[V]]]

  class HostCollectionImpl[V <: Entity](val name: String, getFX: V => Seq[String])(implicit val valuePacker: Packer[V]) {
    host =>

    @volatile private var _meta: DBType.CollectionMeta = null
    private var dvtCount = 0
    private var dvCount = 0
    private val trsCache = new AtomicReference[TraceResults]
    private val listeners = scala.collection.mutable.AnyRefMap[EntityCollectionListener[_], ListenerInfo]()
    @volatile private var deleteListeners = emptyListeners[V]
    @volatile private var updateListeners = emptyListeners[V]
    @volatile private var insertListeners = emptyListeners[V]
    private val indexes = collection.mutable.Map[String, AbstractIndexImpl[V]]()
    val rootTracer = new RootTracer(this)

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
          hostsByID(_meta.prefix) = this
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

    final def getReverseIndex[R <: Entity](name: String): RefReverseIndex[V, R] = {
      indexes.synchronized {
        indexes.get(name).get.asInstanceOf[RefReverseIndex[V, R]]
      }
    }

    @inline final def newDataViewTracerID() = {
      val id = dvtCount
      dvtCount += 1
      id
    }

    @inline final def newDataViewID() = {
      val id = dvCount
      dvCount += 1
      id
    }

    @inline final def allocTraceResults() = {
      val cache = trsCache.getAndSet(null)
      if (cache ne null) cache.reset() else new TraceResults(dvtCount, dvCount)
    }

    @inline final def releaseTraceResults(trs: TraceResults): Unit = {
      trsCache.compareAndSet(null, trs)
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
      if (getFX ne null) {
        FullTextSearchIndex.update(oldEntity.id, getFX(oldEntity), getFX(newEntity))
      }
    }

    @inline final def notifyInsert(newEntity: V)(implicit db: DBSessionUpdatable): Unit = {
      val uls = insertListeners
      var i = uls.length - 1
      while (i >= 0) {
        uls(i).onEntityInsert(newEntity)
        i -= 1
      }
      if (getFX ne null) {
        FullTextSearchIndex.update(newEntity.id, null, getFX(newEntity))
      }
    }

    @inline final def notifyDelete(id: Long)(implicit db: DBSessionUpdatable): Boolean = {
      val uls = insertListeners
      var i = uls.length - 1
      if (i >= 0 || (getFX ne null)) {
        val d = get0(id)
        if (d.isEmpty) {
          return false
        }
        val oldEntity = d.get

        while (i >= 0) {
          uls(i).onEntityDelete(oldEntity)
          i -= 1
        }
        if (getFX ne null) {
          FullTextSearchIndex.update(id, getFX(oldEntity), null)
        }
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

          @inline final override def apply(key: K)(implicit db: DBSessionQueryable): Option[V] = {
            db.get[FullKey, Long](this.getIndexID, key).flatMap(db.get[Long, V])
          }

          @inline final override def delete(key: K)(implicit db: DBSessionUpdatable): Unit = {
            db.get[FullKey, Long](this.getIndexID, key).foreach(host.delete)
          }

          @inline final def update(value: V)(implicit db: DBSessionUpdatable): Unit = {
            host.update(value)
          }

          implicit val cursorKeyRangePacker = Packer.of[(Int, RangeBound[K])]

          override def apply(range: Range[K], forward: Boolean)(implicit db: DBSessionQueryable): Cursor[V] = {
            val r = range.map(r => (this.getIndexID, r))
            Cursor[Long](r, forward).map(id => host.get1(id))
          }
        }
      }
    }

    final def defMKIndex[K: Packer](indexName: String, keyGetter: V => Seq[K]): Index[K, V] = {
      val dv = rootTracer.defDataView.newDataView0[K, Boolean](indexName) { dv =>
        val r = dv.newViewTracer(v => v)
        v => keyGetter(v(r)).map(k => (k, true)).toMap
      }
      new Index[K, V] {
        override def apply(range: Range[K], forward: Boolean)(implicit db: DBSessionQueryable): Cursor[V] = {
          dv(range, forward).map(v => host.get1(v._3.id))
        }
      }
    }

    final def defIndex[K: Packer](indexName: String, keyGetter: V => K): Index[K, V] = {
      type FullKey = (Int, K, Long)
      newIndex(indexName) {
        new IndexImpl[FullKey, V](this, name) with Index[K, V] {
          override def v2fk(value: V)(implicit db: DBSessionQueryable) = (this.getIndexID, keyGetter(value), value.id)

          val cursorKeyRangePacker = Packer.of[(Int, RangeBound[K])]

          override def apply(range: Range[K], forward: Boolean)(implicit db: DBSessionQueryable): Cursor[V] = {
            val r = range.map(k => (this.getIndexID, k))(cursorKeyRangePacker)
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

  import scala.reflect.runtime.universe._

  class SubHostCollectionImpl[O <: Entity, V <: Entity : Packer](name: String, getFX: SCE[O, V] => Seq[String])
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

    trait SubIndex[K] extends AbstractIndexImpl[SCE[O, V]] {
      val cursorKeyRangePacker: Packer[(Int, Long, RangeBound[K])]
    }

    def defUniqueIndex[K: Packer](owner: Ref[O], indexName: String, keyGetter: V => K): UniqueIndex[K, V] = {
      type FullKey = (Int, Long, K)
      val rawIndex = getOrNewIndex(indexName) {
        new UniqueIndexImpl[FullKey, SCE[O, V]](this, indexName) with SubIndex[K] {
          override def v2fk(value: SCE[O, V])(implicit db: DBSessionQueryable) = (this.getIndexID, value.owner.id, keyGetter(value.entity))

          override val cursorKeyRangePacker = Packer.of[(Int, Long, RangeBound[K])]
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

        override def apply(range: Range[K], forward: Boolean)(implicit db: DBSessionQueryable): Cursor[V] = {
          val r = range.map(r => (rawIndex.getIndexID, owner.id, r))(rawIndex.cursorKeyRangePacker)
          Cursor[Long](r, forward).map(id => subHost.get1(id).entity)
        }
      }
    }

    final def defMKIndex[K: Packer](owner: Ref[O], indexName: String, keyGetter: V => Seq[K]): Index[K, V] = {
      val dv = rootTracer.defDataView.newDataView0[(Long, K), Boolean](indexName) { dv =>
        val r = dv.newViewTracer(v => v)
        v => {
          val e = v(r)
          keyGetter(e.entity).map(k => ((e.owner.id, k), true)).toMap
        }
      }
      new Index[K, V] {
        override def apply(range: Range[K], forward: Boolean)(implicit db: DBSessionQueryable): Cursor[V] = {
          dv.scan(range.map(k => (owner.id, k)), forward).map(v => subHost.get1(v._3.id).entity)
        }
      }
    }

    final def defIndex[K: Packer](owner: Ref[O], indexName: String, keyGetter: V => K): Index[K, V] = {
      type FullKey = (Int, Long, K, Long)
      val rawIndex = getOrNewIndex(indexName) {
        new IndexImpl[FullKey, SCE[O, V]](this, indexName) with SubIndex[K] {
          override def v2fk(value: SCE[O, V])(implicit db: DBSessionQueryable) = (this.getIndexID, value.owner.id, keyGetter(value.entity), value.id)

          override val cursorKeyRangePacker = Packer.of[(Int, Long, RangeBound[K])]
        }
      }
      new Index[K, V] {
        override def apply(range: Range[K], forward: Boolean)(implicit db: DBSessionQueryable): Cursor[V] = {
          val r = range.map(r => (rawIndex.getIndexID, owner.id, r))(rawIndex.cursorKeyRangePacker)
          new RawKVCursor[V](r, forward = true) {
            override def buildValue(): V = {
              subHost.get1(rawIndex.fullKeyPacker.unapply(rawKey)._4).entity
            }
          }
        }
      }
    }
  }

  type DVI = DataViewBaseImpl[_, _, _]

  class DataViewTracer[E <: Entity, T](val Tracer: Tracer[E], val trace: E => T, val dataView: DVI, val id: Int)

  class TraceResults(dvTracerCount: Int, dvCount: Int) {
    var oldOrNew: Int = 0

    @inline final def use(useOld: Boolean): Unit = {
      oldOrNew = if (useOld) 0 else 1
    }

    private val oldAndNew = new Array[Any](dvTracerCount * 2)
    private val dvs = new Array[DVI](dvCount)
    var changed: Boolean = false


    @inline final def reset() = {
      if (changed) {
        var i = dvCount - 1
        do {
          dvs(i) = null
          i -= 1
        } while (i >= 0)
        changed = false
      }
      this
    }

    @inline final def mark(dataView: DVI): Unit = {
      dvs(dataView.id) = dataView
      changed = true
    }

    @inline final def tracing(dataView: DVI) = dvs(dataView.id) ne null


    @inline final def trace[E <: Entity, T](tracer: DataViewTracer[E, T], oldEntity: E, newEntity: E, init: Boolean): Boolean = {
      if (init) {
        val oldTR = tracer.trace(oldEntity)
        val newTR = tracer.trace(newEntity)
        val id = tracer.id
        oldAndNew(id * 2) = oldTR
        oldAndNew(id * 2 + 1) = newTR
        if (oldTR != newTR) {
          val dv = tracer.dataView
          dvs(dv.id) = dv
          changed = true
          return true
        }
      } else if (dvs(tracer.dataView.id) ne null) {
        val id = tracer.id
        val otr = tracer.trace(oldEntity)
        oldAndNew(id * 2) = otr
        oldAndNew(id * 2 + 1) = if (oldEntity eq newEntity) otr else tracer.trace(newEntity)
        return true
      }
      false
    }


    @inline final def apply[T](id: Int): T = {
      oldAndNew(id * 2 + oldOrNew).asInstanceOf[T]
    }

    @inline final def apply[T](dvt: DataViewTracer[_, T]): T = {
      oldAndNew(dvt.id * 2 + oldOrNew).asInstanceOf[T]
    }

    @inline final def updateDataView(entityID: Long)(implicit db: DBSessionUpdatable): Unit = {
      var i = dvCount - 1
      do {
        val dv = dvs(i)
        if (dv ne null) {
          dv.update(this, entityID)
        }
        i -= 1
      } while (i >= 0)
    }
  }

  abstract class Tracer[E <: Entity](val root: HostCollectionImpl[_], val host: HostCollectionImpl[E])
    extends HostListening[E] {
    tracer =>

    @inline final def get(id: Long)(implicit db: DBSessionQueryable): E = {
      if (id == 0)
        null.asInstanceOf[E]
      else {
        host.get1(id)
      }
    }

    @inline final def traceSelfAndSub(oldEntity: E, newEntity: E, trs: TraceResults, excludeSubTracer: AnyRef, init: Boolean)(implicit db: DBSessionQueryable): Unit = {
      var i = dataViewTracerCount - 1
      while (i >= 0) {
        trs.trace(dataViewTracers(i), oldEntity, newEntity, init)
        i -= 1
      }
      i = subRefCount - 1
      while (i >= 0) {
        val st = subRefs(i)
        if (st ne excludeSubTracer) {
          st.traceDown(oldEntity, newEntity, trs, init)
        }
        i -= 1
      }
    }

    final override def onEntityInsert(newEntity: E)(implicit db: DBSessionUpdatable): Unit = {
      onEntityUpdate(null.asInstanceOf[E], newEntity)
    }

    final override def onEntityDelete(oldEntity: E)(implicit db: DBSessionUpdatable): Unit = {
      onEntityUpdate(oldEntity, null.asInstanceOf[E])
    }

    final override def onEntityUpdate(oldEntity: E, newEntity: E)(implicit db: DBSessionUpdatable): Unit = {
      val trs = root.allocTraceResults()
      try {
        traceSelfAndSub(oldEntity, newEntity, trs, null, init = true)
        if (trs.changed) {
          traceEnd(oldEntity.id, trs)
        }
      } finally {
        root.releaseTraceResults(trs)
      }
    }

    def traceEnd(entityID: Long, trs: TraceResults)(implicit db: DBSessionUpdatable): Unit = {
      trs.updateDataView(entityID)
    }

    var subRefs: Array[SubTracer[E, _]] = null
    var subRefCount = 0

    final def ensureSubTracer[S <: Entity](refName: String, dataView: DVI): SubTracer[E, S] = {
      var i = subRefCount - 1
      while (i >= 0) {
        val sub = subRefs(i)
        if (sub.reverseIndex.name == refName) {
          return sub.ensureLink(dataView).asInstanceOf[SubTracer[E, S]]
        }
        i -= 1
      }
      val reverseIndex = host.getReverseIndex[S](refName)
      val sub = new SubTracer[E, S](this, reverseIndex)
      if (subRefCount == 0) {
        subRefs = new Array[SubTracer[E, _]](4)
      } else if (subRefCount == subRefs.length) {
        val newArray = new Array[SubTracer[E, _]](subRefCount * 2)
        System.arraycopy(subRefs, 0, newArray, 0, subRefCount)
        subRefs = newArray
      }
      subRefs(subRefCount) = sub
      subRefCount += 1
      sub.ensureLink(dataView)
    }

    var dataViewTracers: Array[DataViewTracer[E, _]] = null
    var dataViewTracerCount = 0

    final def newViewTracer[T](trace: E => T, dataView: DVI): DataViewTracer[E, T] = {
      if (dataViewTracerCount == 0) {
        dataViewTracers = new Array[DataViewTracer[E, _]](4)
      } else if (dataViewTracerCount == dataViewTracers.length) {
        val newArray = new Array[DataViewTracer[E, _]](dataViewTracerCount * 2)
        System.arraycopy(dataViewTracers, 0, newArray, 0, dataViewTracerCount)
        dataViewTracers = newArray
      }
      val dvt = new DataViewTracer[E, T](this, trace, dataView, root.newDataViewTracerID())
      dataViewTracers(dataViewTracerCount) = dvt
      dataViewTracerCount += 1
      dvt
    }
  }

  class SubTracer[P <: Entity, S <: Entity](parent: Tracer[P], val reverseIndex: RefReverseIndex[P, S])
    extends Tracer[S](parent.root, reverseIndex.getRefCollection) {

    final override def canListenEntityInsert: Boolean = false

    final override def canListenEntityDelete: Boolean = false

    override def traceEnd(entityID: Long, trs: TraceResults)(implicit db: DBSessionUpdatable): Unit = {
      for (u <- reverseIndex.scan(entityID)) {
        parent.traceSelfAndSub(u, u, trs, this, init = false)
        parent.traceEnd(u.id, trs)
      }
    }

    @inline final def subID(entity: P): Long = {
      if (entity ne null) {
        val ref = reverseIndex.getRef(entity)
        if (ref ne null) {
          return ref.id
        }
      }
      0
    }

    @inline final def traceDown(oldEntity: P, newEntity: P, trs: TraceResults, init: Boolean)(implicit db: DBSessionQueryable): Unit = {
      val oid: Long = subID(oldEntity)
      var nid: Long = 0
      if (oldEntity ne newEntity) {
        nid = subID(newEntity)
        if (oid != nid && init) {
          var i = dataViewLinkCount - 1
          do {
            trs.mark(dataViewLinks(i))
            i -= 1
          } while (i >= 0)
        }
      } else {
        nid = oid
      }
      if (!init) {
        var i = dataViewLinkCount - 1
        do {
          i = if (trs.tracing(dataViewLinks(i))) -2 else i - 1
        } while (i >= 0)
        if (i == -2) {
          return
        }
      }
      val os = get(oid)
      val ns = if (oid != nid) get(nid) else os
      traceSelfAndSub(os, ns, trs, null, init = false)
    }

    var dataViewLinks: Array[DVI] = null
    var dataViewLinkCount = 0

    def ensureLink(dataView: DVI): this.type = {
      var i = dataViewLinkCount - 1
      if (i < 0) {
        dataViewLinks = new Array[DVI](4)
        dataViewLinks(0) = dataView
        dataViewLinkCount = 1
        return this
      }
      do {
        if (dataViewLinks(i) eq dataView) {
          return this
        }
        i -= 1
      } while (i >= 0)
      if (dataViewLinkCount == dataViewLinks.length) {
        val newArray = new Array[DVI](dataViewLinkCount * 2)
        System.arraycopy(dataViewLinks, 0, newArray, 0, dataViewLinkCount)
        dataViewLinks = newArray
      }
      dataViewLinks(dataViewLinkCount) = dataView
      dataViewLinkCount += 1
      this

    }
  }

  class DataViewBaseImpl[DK: Packer, DV: Packer, E <: Entity](val host: HostCollectionImpl[E], val name: String, val id: Int,
                                                              builder: (DataViewBaseImpl[DK, DV, E]) => TraceResults => Map[DK, DV], uniqueKey: Boolean)
    extends AbstractIndexImpl[E] with UniqueKeyView[DK, DV, E] {

    final def newViewTracer[T](trace: E => T) = {
      host.rootTracer.newViewTracer(trace, this)
    }

    final def newViewTracer[TE <: Entity, T](path: String, trace: TE => T) = {
      var tracer: Tracer[_] = host.rootTracer
      for (refName <- path.split(".")) {
        tracer = tracer.ensureSubTracer(refName, this)
      }
      tracer.asInstanceOf[Tracer[TE]].newViewTracer(trace, this)
    }

    val kp_u = if (uniqueKey) Packer.of[(Int, DK)] else null
    val kp = if (uniqueKey) null else Packer.of[(Int, DK, Long)]
    val vp_u = if (uniqueKey) Packer.of[(Long, DV)] else null
    val vp_uv = if (uniqueKey) Packer.map[DV, (Long, DV)]((0, _), _._2) else null
    val vp_ur = if (uniqueKey) Packer.of[(Ref[E], DV)] else null
    val vp = if (uniqueKey) null else Packer.of[DV]

    val extract = builder(this)

    def update(trs: TraceResults, entityID: Long)(implicit db: DBSessionUpdatable): Unit = {
      trs.use(useOld = true)
      val oldKV = extract(trs)
      trs.use(useOld = false)
      val newKV = extract(trs)
      for ((k, v) <- oldKV) {
        val newVO = newKV.get(k)
        if (newVO.isEmpty) {
          del(k, entityID)
        } else if (newVO.get != v) {
          put(k, v, entityID)
        }
      }
      for ((k, v) <- newKV) {
        if (oldKV.get(k).isEmpty) {
          put(k, v, entityID)
        }
      }
    }

    @inline final def put(k: DK, v: DV, entityID: Long)(implicit db: DBSessionUpdatable) = {
      if (uniqueKey) {
        db.put((getIndexID, k), (entityID, v))(kp_u, vp_u)
      } else {
        db.put((getIndexID, k, entityID), v)(kp, vp)
      }
    }

    @inline final def del(k: DK, entityID: Long)(implicit db: DBSessionUpdatable) = {
      if (uniqueKey) {
        db.del((getIndexID, k))(kp_u)
      } else {
        db.del((getIndexID, k, entityID))(kp)
      }
    }

    val cursorRangePacker = Packer.of[(Int, RangeBound[DK])]

    final def apply(key: DK)(implicit db: DBSessionQueryable): Option[DV] = {
      if (uniqueKey) {
        db.get((getIndexID, key))(kp_u, vp_uv)
      } else {
        throw new UnsupportedOperationException
      }
    }

    final def getRV(key: DK)(implicit db: DBSessionQueryable): Option[(Ref[E], DV)] = {
      if (uniqueKey) {
        db.get((getIndexID, key))(kp_u, vp_ur)
      } else {
        throw new UnsupportedOperationException
      }
    }

    final def scan(dbRange: DBRange, forward: Boolean = true)(implicit db: DBSessionQueryable): RawKVCursor[(DK, DV, Ref[E])] = {
      if (uniqueKey) {
        new RawKVCursor[(DK, DV, Ref[E])](dbRange, forward) {

          override def buildValue(): (DK, DV, Ref[E]) = {
            val rk = kp_u.unapply(rawKey)
            val rv = vp_u.unapply(rawValue)
            (rk._2, rv._2, RefSome(rv._1))
          }
        }
      } else {
        new RawKVCursor[(DK, DV, Ref[E])](dbRange, forward) {

          override def buildValue(): (DK, DV, Ref[E]) = {
            val rk = kp.unapply(rawKey)
            val rv = vp.unapply(rawValue)
            (rk._2, rv, RefSome(rk._3))
          }
        }
      }
    }

    final def apply(range: Range[DK], forward: Boolean = true)(implicit db: DBSessionQueryable): Cursor[(DK, DV, Ref[E])] = {
      val r = range.map(r => (getIndexID, r))(cursorRangePacker)
      scan(range, forward)
    }
  }

  class RootTracer[E <: Entity](host: HostCollectionImpl[E]) extends Tracer[E](host, host) with HostListening[E] {

    val defUniqueKeyView = new DataViewBuilderImpl[E, UniqueKeyView](host, true)

    val defDataView = new DataViewBuilderImpl[E, DataView](host, false)
  }

  import scala.language.higherKinds

  class DataViewBuilderImpl[E <: Entity, DW[_, _, _ <: Entity]](host: HostCollectionImpl[E], uniqueKey: Boolean) extends DataViewBuilder[E, DW] {
    final def newDataView0[DK: Packer, DV: Packer](indexName: String)(builder: (DataViewBaseImpl[DK, DV, E]) => TraceResults => Map[DK, DV]): DataViewBaseImpl[DK, DV, E] = {
      host.newIndex(indexName) {
        new DataViewBaseImpl[DK, DV, E](host, indexName, host.newDataViewID(), builder, uniqueKey)
      }
    }

    final def newDataView[DK: Packer, DV: Packer](indexName: String)(builder: (DataViewBaseImpl[DK, DV, E]) => TraceResults => Map[DK, DV]) = {
      newDataView0(indexName)(builder).asInstanceOf[DW[DK, DV, E]]
    }

    override def apply[DK: Packer, DV: Packer, T](name: String, trace: (E) => T)(extract: (T) => Map[DK, DV]) = {
      newDataView[DK, DV](name) { dv =>
        val t = dv.newViewTracer(trace)
        (tr) => {
          extract(tr(t))
        }
      }
    }

    override def apply[DK: Packer, DV: Packer, T, R1, R1T](name: String, trace: (E) => T,
                                                           r1Path: String, r1Trace: (R1) => R1T)
                                                          (extract: (T, R1T) => Map[DK, DV]) = {
      newDataView[DK, DV](name) { dv =>
        val t = dv.newViewTracer(trace)
        val t1 = dv.newViewTracer(r1Path, r1Trace)
        (tr) => {
          extract(tr(t), tr(t1))
        }
      }
    }

    override def apply[DK: Packer, DV: Packer, T, R1, R1T, R2, R2T](name: String, trace: (E) => T, r1Path: String, r1Trace: (R1) => R1T, r2Path: String, r2Trace: (R2) => R2T)(extract: (T, R1T, R2T) => Map[DK, DV]) = {
      newDataView[DK, DV](name) { dv =>
        val t = dv.newViewTracer(trace)
        val t1 = dv.newViewTracer(r1Path, r1Trace)
        val t2 = dv.newViewTracer(r2Path, r2Trace)
        (tr) => {
          extract(tr(t), tr(t1), tr(t2))
        }
      }
    }

    override def apply[DK: Packer, DV: Packer, T, R1, R1T, R2, R2T, R3, R3T](name: String, trace: (E) => T,
                                                                             r1Path: String, r1Trace: (R1) => R1T,
                                                                             r2Path: String, r2Trace: (R2) => R2T,
                                                                             r3Path: String, r3Trace: (R3) => R3T)
                                                                            (extract: (T, R1T, R2T, R3T) => Map[DK, DV]) = {
      newDataView[DK, DV](name) { dv =>
        val t = dv.newViewTracer(trace)
        val t1 = dv.newViewTracer(r1Path, r1Trace)
        val t2 = dv.newViewTracer(r2Path, r2Trace)
        val t3 = dv.newViewTracer(r3Path, r3Trace)
        (tr) => {
          extract(tr(t), tr(t1), tr(t2), tr(t3))
        }
      }
    }

    override def apply[DK: Packer, DV: Packer, T, R1, R1T, R2, R2T, R3, R3T, R4, R4T](name: String, trace: (E) => T,
                                                                                      r1Path: String, r1Trace: (R1) => R1T,
                                                                                      r2Path: String, r2Trace: (R2) => R2T,
                                                                                      r3Path: String, r3Trace: (R3) => R3T,
                                                                                      r4Path: String, r4Trace: (R4) => R4T)
                                                                                     (extract: (T, R1T, R2T, R3T, R4T) => Map[DK, DV]) = {
      newDataView[DK, DV](name) { dv =>
        val t = dv.newViewTracer(trace)
        val t1 = dv.newViewTracer(r1Path, r1Trace)
        val t2 = dv.newViewTracer(r2Path, r2Trace)
        val t3 = dv.newViewTracer(r3Path, r3Trace)
        val t4 = dv.newViewTracer(r4Path, r4Trace)
        (tr) => {
          extract(tr(t), tr(t1), tr(t2), tr(t3), tr(t4))
        }
      }
    }

    override def apply[DK: Packer, DV: Packer, T, R1, R1T, R2, R2T, R3, R3T, R4, R4T, R5, R5T](name: String, trace: (E) => T,
                                                                                               r1Path: String, r1Trace: (R1) => R1T,
                                                                                               r2Path: String, r2Trace: (R2) => R2T,
                                                                                               r3Path: String, r3Trace: (R3) => R3T,
                                                                                               r4Path: String, r4Trace: (R4) => R4T,
                                                                                               r5Path: String, r5Trace: (R5) => R5T)
                                                                                              (extract: (T, R1T, R2T, R3T, R4T, R5T) => Map[DK, DV]) = {
      newDataView[DK, DV](name) { dv =>
        val t = dv.newViewTracer(trace)
        val t1 = dv.newViewTracer(r1Path, r1Trace)
        val t2 = dv.newViewTracer(r2Path, r2Trace)
        val t3 = dv.newViewTracer(r3Path, r3Trace)
        val t4 = dv.newViewTracer(r4Path, r4Trace)
        val t5 = dv.newViewTracer(r5Path, r5Trace)
        (tr) => {
          extract(tr(t), tr(t1), tr(t2), tr(t3), tr(t4), tr(t5))
        }
      }
    }
  }


  trait HostListening[E <: Entity] extends EntityCollectionListener[E] {
    val host: HostCollectionImpl[E]
    host.addListener(this)
  }

  trait AbstractIndexImpl[E <: Entity] {
    val host: HostCollectionImpl[E]
    val name: String
    var _meta: IndexMeta = null

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

  abstract class IndexImpl[FK, V <: Entity : Packer](val host: HostCollectionImpl[V], val name: String)(implicit val fullKeyPacker: Packer[FK])
    extends AbstractIndexImpl[V] with HostListening[V] {
    index =>

    def v2fk(value: V)(implicit db: DBSessionQueryable): FK

    def onEntityInsert(newEntity: V)(implicit db: DBSessionUpdatable): Unit = {
      db.put(v2fk(newEntity), true)
    }

    def onEntityDelete(oldEntity: V)(implicit db: DBSessionUpdatable): Unit = {
      db.del(v2fk(oldEntity))
    }

    def onEntityUpdate(oldEntity: V, newEntity: V)(implicit db: DBSessionUpdatable): Unit = {
      val oldIndexEntryKey = v2fk(oldEntity)
      val newIndexEntryKey = v2fk(newEntity)
      if (oldIndexEntryKey != newIndexEntryKey) {
        db.del(oldIndexEntryKey)
        db.put(newIndexEntryKey, true)
      }
    }

  }


  trait RefReverseIndex[V <: Entity, R <: Entity] {
    val name: String

    def getRef(entity: V): Ref[R]

    val host: HostCollectionImpl[V]

    def getRefCollection: HostCollectionImpl[R]

    def scan(refID: Long): Cursor[V]
  }

  object RefReverseIndex {
    implicit val fullKeyPacker = Packer.of[(Long, Long)]

    @inline final def update(oldRID: Long, newRID: Long, id: Long)(implicit db: DBSessionUpdatable): Unit = {
      if (oldRID != newRID) {
        if (oldRID != 0) {
          db.del((oldRID, id))
        }
        if (newRID != 0) {
          db.put((newRID, id), true)
        }
      }
    }

    @inline final def scan[V <: Entity](id: Long, host: HostCollectionImpl[V])(implicit db: DBSessionQueryable): Cursor[V] = {
      import Range._
      val idMin = if (host == null) host.fixID(0) else 0L
      val idMax = if (host == null) host.fixID(Long.MaxValue) else Long.MaxValue
      new RawKVCursor[V]((id, idMin, 0L) +-+(id, idMax, Long.MaxValue), forward = true) {
        override def buildValue() = {
          getEntity[V](fullKeyPacker.unapply(rawKey)._2)
        }
      }
    }
  }


  object FullTextSearchIndex {

    val emptyStringSet = Set[String]()

    implicit val fullKeyPacker = Packer.of[(String, Long)]

    val kc = new JiebaSegmenter()

    def toWords(text: Seq[String]): scala.collection.Set[String] = {
      if (text ne null) {
        val sb = new StringBuilder()
        for (s <- text) {
          sb.append(s).append(' ')
        }
        if (sb.length > 0) {
          val words = kc.sentenceProcess(sb.toString())
          var i = words.size() - 1
          if (i >= 0) {
            val words2 = scala.collection.mutable.Set[String]()
            do {
              words2.add(words.get(i).getToken)
              i -= 1
            } while (i >= 0)
            return words2
          }
        }
      }
      emptyStringSet
    }

    def update(entityID: Long, oldText: Seq[String], newText: Seq[String])(implicit db: DBSessionUpdatable): Unit = {
      val oldWords = toWords(oldText)
      val newWords = toWords(newText)
      for (w <- newWords) {
        if (!oldWords.contains(w)) {
          db.put((w, entityID), true)
        }
      }
      for (w <- oldWords) {
        if (!newWords.contains(w)) {
          db.del((w, entityID))
        }
      }
    }

    @inline final def scan[V <: Entity](word: String, host: HostCollectionImpl[V])(implicit db: DBSessionQueryable): Cursor[V] = {
      import Range._
      val cid = host.getCollectionID
      val idMin = if (host == null) host.fixID(0) else 0
      val idMax = if (host == null) host.fixID(Long.MaxValue) else Long.MaxValue
      new RawKVCursor[V]((cid, word, idMin) +-+(cid, word, idMax), forward = true) {
        override def buildValue(): V = {
          host.get1(fullKeyPacker.unapply(rawKey)._2)
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

