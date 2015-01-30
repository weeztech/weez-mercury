package com.weez.mercury.common


import java.util

import com.weez.mercury.common.Cursor.CursorImpl
import com.weez.mercury.common.DataView.Tracer

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

    final def getReverseIndex[R <: Entity](name: String): RefReverseIndex[R] = {
      indexes.synchronized[RefReverseIndex[R]] {
        indexes.get(name).get.asInstanceOf[RefReverseIndex[R]]
      }
    }

    trait AbstractIndexImpl extends EntityCollectionListener[V] {
      val name: String
      var indexID = 0

      def getIndexID()(implicit db: DBSessionQueryable) = {
        if (this.indexID == 0) {
          this.indexID = host.getIndexID(this.name)
        }
        this.indexID
      }

      host.addListener(this)
    }

    abstract class UniqueIndexImpl[FK](val name: String)(implicit fullKeyPacker: Packer[FK])
      extends AbstractIndexImpl {
      index =>

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

    abstract class IndexImpl[FK](val name: String)(implicit val fullKeyPacker: Packer[FK])
      extends AbstractIndexImpl {
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

    type RefReverseIndexFullKey = (Long, Int, Int, Long)

    class RefReverseIndex[R <: Entity](val name: String) extends AbstractIndexImpl {
      implicit val fullKeyPacker = Packer.of[RefReverseIndexFullKey]

      def getRefCollection: HostCollectionImpl[R] = ???

      val getRef: (V) => Ref[R] = ???

      @inline final def updateIndex(oldRID: Long, newRID: Long, id: Long)(implicit db: DBSessionUpdatable): Unit = {
        if (oldRID != newRID) {
          val tableID = host.meta.prefix
          val indexID = getIndexID()
          if (oldRID != 0) {
            db.del[RefReverseIndexFullKey]((oldRID, tableID, indexID, id))
          }
          if (newRID != 0) {
            db.put[RefReverseIndexFullKey, Boolean]((oldRID, tableID, indexID, id), true)
          }
        }
      }

      @inline final def getRefID(newEntity: V): Long = {
        val r = getRef(newEntity)
        if (r eq null) 0 else r.id
      }

      def onEntityInsert(newEntity: V)(implicit db: DBSessionUpdatable): Unit = {
        updateIndex(0, getRefID(newEntity), newEntity.id)
      }

      def onEntityDelete(oldEntity: V)(implicit db: DBSessionUpdatable): Unit = {
        updateIndex(getRefID(oldEntity), 0, oldEntity.id)
      }

      def onEntityUpdate(oldEntity: V, newEntity: V)(implicit db: DBSessionUpdatable): Unit = {
        updateIndex(getRefID(oldEntity), getRefID(newEntity), oldEntity.id)
      }

      @inline final def scan(id: Long)(implicit db: DBSessionQueryable): Cursor[V] = {
        import Range._
        val tableID = host.meta.prefix
        val indexID = getIndexID()
        Cursor.raw[Boolean]((id, tableID, indexID, 0L) +--(id, tableID, indexID, Long.MaxValue), forward = true).mapWithKey { (k, _) =>
          host.apply(fullKeyPacker.unapply(k)._3).get
        }
      }
    }

    class DataViewImpl[K, IV, RTR](val name: String, trace: V => RTR, extract: (RTR, TraceResults) => Map[K, IV])
                                  (implicit kPacker: Packer[K], vPacker: Packer[IV], fullKeyPacker: Packer[(Int, K)])
      extends TracerImpl[V, RTR](trace, host) with AbstractIndexImpl with DVI {


      override val dvi: DVI = this

      @inline final def apply(key: K)(implicit db: DBSessionQueryable): Option[IV] = get(key)

      @inline final def apply(range: Range[K], forward: Boolean = true)(implicit db: DBSessionQueryable): Cursor[(K, IV)] = {
        val r = range.map(r => (getIndexID(), r))(cursorRangePacker)
        Cursor.raw[IV](r, forward).mapWithKey((k, v) => (fullKeyPacker.unapply(k)._2, v))
      }

      private type FullKey = (Int, K)
      private val cursorRangePacker = Packer.of[(Int, RangeBound[K])]

      @inline final def traceSubs(r: V)(implicit db: DBSessionQueryable): TraceResults = {
        if (this.subRefs ne null) {
          val trs = TraceResults()
          traceSubs(r, r, trs, null)
          trs
        } else {
          emptyTraceResults
        }
      }

      override final def onEntityInsert(newEntity: V)(implicit db: DBSessionUpdatable): Unit = {
        for ((k, v) <- extract(trace(newEntity), traceSubs(newEntity))) {
          db.put(k, v)
        }
      }

      override final def onEntityDelete(oldEntity: V)(implicit db: DBSessionUpdatable): Unit = {
        for ((k, _) <- extract(trace(oldEntity), traceSubs(oldEntity))) {
          db.del(k)
        }
      }

      override def onEntityUpdate(oldEntity: V, newEntity: V)(implicit db: DBSessionUpdatable): Unit = {
        val oldTR = trace(oldEntity)
        val newTR = trace(newEntity)
        if (oldTR != newTR) {
          val oldTRs = traceSubs(oldEntity)
          val newTRs = traceSubs(newEntity)
          val newKV = extract(newTR, newTRs)
          for ((k, v) <- newKV) {
            put(k, v)
          }
          for ((k, _) <- extract(oldTR, oldTRs)) {
            if (newKV.get(k).isEmpty) {
              del(k)
            }
          }
        }
      }

      @inline final def get(k: K)(implicit db: DBSessionQueryable) = {
        db.get((getIndexID(), k))(fullKeyPacker, vPacker)
      }

      @inline final def put(k: K, v: IV)(implicit db: DBSessionUpdatable) = {
        db.put((getIndexID(), k), v)(fullKeyPacker, vPacker)
      }

      @inline final def del(k: K)(implicit db: DBSessionUpdatable) = {
        db.del((getIndexID(), k))(fullKeyPacker)
      }

      final override def traceSource(entity: V, trs: TraceResults)(implicit db: DBSessionUpdatable): Unit = {
        val tr = trace(entity)
        trs.use(useOld = true)
        val newKV = extract(tr, trs)
        for ((k, v) <- newKV) {
          put(k, v)
        }
        trs.use(useOld = false)
        for ((k, _) <- extract(tr, trs)) {
          if (newKV.get(k).isEmpty) {
            del(k)
          }
        }
      }

      final override def canListenEntityUpdate: Boolean = true

      final override def canListenEntityInsert: Boolean = true

      final override def canListenEntityDelete: Boolean = true

      host.addListener(this)
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
      Cursor.raw[V](entityIDOf(prefix, 0) +-+ entityIDOf(prefix, -1L), forward).mapWithKey((k, v) => {
        v._id = Packer.LongPacker.unapply(k);
        v
      })
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

    val indexes = collection.mutable.Map[String, AbstractIndexImpl]()

    def defUniqueIndex[K: Packer : TypeTag](indexName: String, keyGetter: V => K): UniqueIndex[K, V] = {
      type FullKey = (Int, K)
      this.indexes.synchronized[UniqueIndex[K, V]] {
        if (this.indexes.get(indexName).isDefined) {
          throw new IllegalArgumentException( s"""index naming "$indexName" exist!""")
        }
        val idx = new UniqueIndexImpl[FullKey](indexName) with UniqueIndex[K, V] {

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

          val cursorKeyRangePacker = Packer.of[(Int, RangeBound[K])]

          override def apply(range: Range[K], forward: Boolean)(implicit db: DBSessionQueryable): Cursor[V] = {
            val r = range.map(r => (this.getIndexID, r))(cursorKeyRangePacker)
            Cursor[Long](r, forward).map(id => host(id).get)
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
        val idx = new IndexImpl[FullKey](name) with Index[K, V] {
          override def v2fk(value: V)(implicit db: DBSessionQueryable) = (this.getIndexID, keyGetter(value), value.id)

          val cursorKeyRangePacker = Packer.of[(Int, RangeBound[K])]


          override def apply(range: Range[K], forward: Boolean)(implicit db: DBSessionQueryable): Cursor[V] = {
            val r = range.map(r => (this.getIndexID, r))(cursorKeyRangePacker) //TODO prefix
            Cursor[Long](r, forward).map(id => host(id).get)
          }
        }
        this.indexes.put(name, idx)
        idx
      }
    }

    def defDataView[K: Packer, IV: Packer, RTR](name: String, trace: V => RTR, extract: (RTR, TraceResults) => Map[K, IV]): DataViewImpl[K, IV, RTR] = {
      this.indexes.synchronized {
        if (this.indexes.contains(name)) {
          throw new IllegalArgumentException( s"""DataView naming "$name" exist!""")
        }
        val view = new DataViewImpl[K, IV, RTR](name, trace, extract)
        this.indexes.put(name, view)
        view
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

    trait SubIndex[K] {
      val cursorKeyRangePacker: Packer[(Int, Ref[O], RangeBound[K])]
    }

    def defUniqueIndex[K: Packer : TypeTag](owner: Ref[O], indexName: String, keyGetter: V => K): UniqueIndex[K, V] = {
      type FullKey = (Int, Ref[O], K)
      indexes.synchronized[UniqueIndex[K, V]] {
        val rawIndex = this.indexes.getOrElseUpdate(indexName,
          new UniqueIndexImpl[FullKey](indexName) with SubIndex[K] {
            override def v2fk(value: SCE[O, V])(implicit db: DBSessionQueryable) = (this.getIndexID, value.owner, keyGetter(value.entity))

            override val cursorKeyRangePacker = Packer.of[(Int, Ref[O], RangeBound[K])]
          }
        ).asInstanceOf[UniqueIndexImpl[FullKey] with SubIndex[K]]
        new UniqueIndex[K, V] {

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
            val r = range.map(r => (rawIndex.getIndexID, owner, r))(rawIndex.cursorKeyRangePacker)
            Cursor[Long](r, forward).map(id => subHost(id).get.entity)
          }
        }
      }
    }

    final def defIndex[K: Packer : TypeTag](owner: Ref[O], indexName: String, keyGetter: V => K): Index[K, V] = {
      type FullKey = (Int, Ref[O], K, Long)
      indexes.synchronized[Index[K, V]] {
        val rawIndex = this.indexes.getOrElseUpdate(indexName,
          new IndexImpl[FullKey](indexName) with SubIndex[K] {
            override def v2fk(value: SCE[O, V])(implicit db: DBSessionQueryable) = (this.getIndexID, value.owner, keyGetter(value.entity), value.id)

            override val cursorKeyRangePacker = Packer.of[(Int, Ref[O], RangeBound[K])]
          }
        ).asInstanceOf[IndexImpl[FullKey] with SubIndex[K]]
        new Index[K, V] {
          override def apply(range: Range[K], forward: Boolean)(implicit db: DBSessionQueryable): Cursor[V] = {
            val r = range.map(r => (rawIndex.getIndexID, owner, r))(rawIndex.cursorKeyRangePacker) //TODO prefix
            Cursor.raw[Long](r, forward).mapWithKey((k, _) => subHost(rawIndex.fullKeyPacker.unapply(k)._4).get.entity)
          }
        }
      }
    }
  }

  class TraceResults(cap: Int) extends DataView.TraceResults {
    var vStart: Int = 0
    private val oldAndNew: Array[Any] = new Array[Any](cap * 2)

    @inline final def use(useOld: Boolean): Unit = {
      vStart = if (useOld) 0 else cap
    }

    @inline final def put[TR](tracer: Tracer[_, TR], oldTR: TR, newTR: TR): Unit = {
      val id = tracer.id
      oldAndNew(id) = oldTR
      oldAndNew(id + cap) = newTR
    }

    def apply[TR](tracer: Tracer[_, TR]): TR = {
      oldAndNew(vStart + tracer.id).asInstanceOf[TR]
    }
  }

  val emptyTraceResults = new TraceResults(0)

  trait DVI {
    val id = Int.MinValue
    var tracerCount = 0

    def TraceResults() = new TraceResults(tracerCount)
  }

  abstract class TracerImpl[E <: Entity, TR](trace: E => TR, val traceCollection: HostCollectionImpl[E])
    extends DataView.Tracer[E, TR] with EntityCollectionListener[E] {
    tracer =>
    def traceSource(entity: E, trs: TraceResults)(implicit db: DBSessionUpdatable): Unit

    @inline final def get(id: Long)(implicit db: DBSessionQueryable): E = {
      if (id == 0)
        null.asInstanceOf[E]
      else {
        val e = traceCollection(id)
        if (e.isEmpty) null.asInstanceOf[E] else e.get
      }
    }

    class SubTracer[S <: Entity, STR](name: String, trace: S => STR, reverseIndex: HostCollectionImpl[E]#RefReverseIndex[S])
      extends TracerImpl[S, STR](trace, reverseIndex.getRefCollection) {
      val dvi = tracer.dvi

      val id = tracer.addSub(this)

      final override def canListenEntityUpdate: Boolean = true

      final override def canListenEntityInsert: Boolean = false

      final override def canListenEntityDelete: Boolean = false

      final override def onEntityInsert(newEntity: S)(implicit db: DBSessionUpdatable): Unit = {
        //doNothing
      }

      final override def onEntityDelete(oldEntity: S)(implicit db: DBSessionUpdatable): Unit = {
        //doNothing
      }

      final override def onEntityUpdate(oldEntity: S, newEntity: S)(implicit db: DBSessionUpdatable): Unit = {
        val oldTR = trace(oldEntity)
        val newTR = trace(newEntity)
        if (oldTR != newTR) {
          val trs = dvi.TraceResults()
          trs.put(this, oldTR, newTR)
          traceSubs(oldEntity, newEntity, trs, null)
          for (u <- reverseIndex.scan(oldEntity.id)) {
            tracer.traceSubs(u, u, trs, this)
            tracer.traceSource(u, trs)
          }
        }
      }

      @inline final def subID(entity: E): Long = {
        if (entity ne null) {
          val ref = reverseIndex.getRef(entity)
          if (ref ne null) {
            return ref.id
          }
        }
        0
      }

      @inline final def traceSelfAndSubs(oldEntity: E, newEntity: E, trs: TraceResults)(implicit db: DBSessionQueryable): Unit = {
        val sid =
          if (oldEntity ne newEntity) {
            val oid = subID(oldEntity)
            val nid = subID(newEntity)
            if (oid != nid) {
              val os = get(oid)
              val ns = get(nid)
              trs.put(this, trace(os), trace(ns))
              traceSubs(os, ns, trs, null)
              return
            }
            oid
          } else {
            subID(oldEntity)
          }
        val s = get(sid)
        val tr = trace(s)
        trs.put(this, tr, tr)
        traceSubs(s, s, trs, null)
      }

      @inline final override def traceSource(entity: S, trs: TraceResults)(implicit db: DBSessionUpdatable): Unit = {
        val tr = trace(entity)
        trs.put(this, tr, tr)
        for (u <- reverseIndex.scan(entity.id)) {
          tracer.traceSubs(u, u, trs, this)
          tracer.traceSource(u, trs)
        }
      }

      traceCollection.addListener(this)
    }

    @inline final def defSubTracer[S <: Entity, STR](name: String, trace: S => STR): SubTracer[S, STR] = {
      val reverseIndex = traceCollection.getReverseIndex[S](name)
      new SubTracer[S, STR](name, trace, reverseIndex)
    }

    var subRefs: Array[SubTracer[_, _]] = null

    @inline final def addSub(sub: SubTracer[_, _]): Int = {
      if (subRefs eq null) {
        subRefs = Array[SubTracer[_, _]](sub)
      } else {
        val l = subRefs.length
        val newSubs = new Array[SubTracer[_, _]](l + 1)
        System.arraycopy(subRefs, 0, newSubs, 0, l)
        subRefs = newSubs
        subRefs(l) = sub
      }
      val i = dvi.tracerCount
      dvi.tracerCount = i + 1
      i
    }

    val dvi: DVI

    @inline final def traceSubs(oldEntity: E, newEntity: E, trs: TraceResults, excludeSubTracer: AnyRef)(implicit db: DBSessionQueryable): Unit = {
      if (subRefs != null) {
        var i = subRefs.length - 1
        while (i >= 0) {
          val st = subRefs(i)
          if (st ne excludeSubTracer) {
            st.traceSelfAndSubs(oldEntity, newEntity, trs)
          }
          i -= 1
        }
      }
    }
  }

}

