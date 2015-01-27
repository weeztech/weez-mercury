package com.weez.mercury.common


import com.weez.mercury.common.EntityCollections.MayBeReverseIndex

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

  def forPartitionHost[V <: Entity : Packer : TypeTag](pc: SubCollection[V]): SubHostCollectionImpl[V] = {
    this.hosts.synchronized {
      this.hosts.get(pc.name) match {
        case Some(host: SubHostCollectionImpl[V]) => host
        case None =>
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
      if (this.hostsByType.contains(typeTag)) {
        throw new IllegalArgumentException( s"""HostCollection typed "$typeTag" exist!""")
      }
      val host = new HostCollectionImpl[V](name) {}
      this.hosts.put(name, host)
      this.hostsByType.put(typeTag, host)
      host
    }
  }

  val hosts = collection.mutable.HashMap[String, HostCollectionImpl[_]]()
  val hostsByID = collection.mutable.HashMap[Int, HostCollectionImpl[_]]()
  val hostsByType = collection.mutable.HashMap[TypeTag[_], HostCollectionImpl[_]]()


//  trait IndexImplBase[V] {
//    def reverseRef(ref: Ref[_]): CursorImpl[_, V]
//  }

  trait MayBeReverseIndex[V <: Entity] {
    def canBeReverseIndex: Boolean

    def scanReverse[R <: Entity](ref: Ref[R])(implicit db: DBSessionQueryable): Cursor[V] = throw new UnsupportedOperationException
  }

  final val RefType = typeOf[Ref[Entity]]

  final val ProductType = typeOf[Product]

  abstract class HostCollectionImpl[V <: Entity : Packer](val name: String) {
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
    @volatile private var listeners2: Seq[EntityCollectionListener[V]] = Seq.empty

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
        listeners2 = listeners.values.map(l => l.l).toSeq
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
        db.get[Long, V](id)
      }
    }

    final def apply()(implicit db: DBSessionQueryable): Cursor[V] = {
      import Range._
      val prefix = this.meta.prefix
      new CursorImpl[V](entityIDOf(prefix, 0) +-+ entityIDOf(prefix, -1L), true)
    }

    @inline final def checkID(id: Long)(implicit db: DBSessionUpdatable): Long = {
      if (collectionIDOf(id) != this.meta.id) {
        throw new IllegalArgumentException("id is not belong this collection")
      }
      id
    }

    final def update(value: V)(implicit db: DBSessionUpdatable): Unit = {
      val id = checkID(value.id)
      lazy val old: Option[V] = db.get[Long, V](id)
      for (l <- this.listeners2) {
        if (old.isDefined) {
          l.onEntityUpdate(old.get, value)
        } else {
          l.onEntityInsert(value)
        }
      }
      db.put(id, value)
    }

    final def delete(id: Long)(implicit db: DBSessionUpdatable): Unit = {
      checkID(id)
      lazy val old: Option[V] = db.get[Long, V](id)
      for (l <- this.listeners2) {
        if (old.isDefined) {
          l.onEntityDelete(old.get)
        }
      }
      db.del(id)
    }

    val indexes = collection.mutable.Map[String, IndexBaseImpl[_]]()

    abstract class HostIndexBaseImpl[FK, K](name: String)(implicit fullKeyPacker: Packer[FK], keyPacker: Packer[K], keyTypeTag: TypeTag[K])
      extends IndexBaseImpl[FK](name)
      with MayBeReverseIndex[V] {

      override def canBeReverseIndex: Boolean = {
        keyTypeTag match {
          case TypeRef(RefType, _, _) => true
          case TypeRef(t, _, args) => t <:< ProductType && args != Nil && args.head <:< RefType
          case _ => false
        }
      }

      override def scanReverse[R <: Entity](ref: Ref[R])(implicit db: DBSessionQueryable): Cursor[V] = {
        keyTypeTag match {
          case TypeRef(RefType, _, _) =>
            val start = (this.getIndexID, ref)
            val end = (this.getIndexID, RefSome[R](ref.id + 1))
            new CursorImpl[Long](Range.BoundaryRange(Range.Include(start), Range.Exclude(end)), forward = true).map { id =>
              host(id).get
            }
          case TypeRef(t, _, args) =>
            if (t <:< ProductType && args != Nil && args.head <:< RefType) {
              val start = (this.getIndexID, Tuple1(ref))
              val end = (this.getIndexID, Tuple1(RefSome[R](ref.id + 1)))
              new CursorImpl[Long](Range.BoundaryRange(Range.Include(start), Range.Exclude(end)), forward = true).map { id =>
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

      def apply(range: Range[K], forward: Boolean)(implicit db: DBSessionQueryable): Cursor[V] = {
        val r = range.map(r => (this.getIndexID, r))(cursorKeyRangePacker)
        new CursorImpl[Long](r, forward) map { id =>
          host(id).get
        }
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
        }
        this.indexes.put(name, idx)
        idx
      }
    }
  }

  class CursorImpl[V](range: DBRange, forward: Boolean)(implicit db: DBSessionQueryable, pv: Packer[V]) extends Cursor[V] {

    import com.weez.mercury.debug._
    import Range.{ByteArrayOrdering => O}

    val step = if (forward) 1 else -1
    val rangeStart = range.keyStart
    val rangeEnd = range.keyEnd
    var dbCursor: DBCursor =
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

    def isValid = {
      dbCursor != null && {
        val valid =
          dbCursor.isValid && {
            if (forward)
              O.compare(dbCursor.key(), rangeEnd) < 0
            else
              O.compare(dbCursor.key(), rangeStart) >= 0
          }
        if (!valid) close()
        valid
      }
    }

    def value = {
      try {
        pv.unapply(dbCursor.value())
      } catch {
        case ex: IllegalArgumentException =>
          println("range : " + range)
          println("key   : " + PackerDebug.show(dbCursor.key()))
          println("value : " + PackerDebug.show(dbCursor.value()))
          throw ex
      }
    }

    def next() = {
      if (dbCursor != null)
        dbCursor.next(step)
    }

    def close() = {
      if (dbCursor != null) {
        dbCursor.close()
        dbCursor = null
      }
    }
  }

  @packable
  case class SCE[V <: Entity](ownerID: Long, v: V) extends Entity {
    @inline override final def id = v.id
  }

  import scala.reflect.runtime.universe._

  class SubHostCollectionImpl[V <: Entity : Packer : TypeTag](name: String) extends HostCollectionImpl[SCE[V]](name) {
    subHost =>
    final def update(ownerID: Long, value: V)(implicit db: DBSessionUpdatable): Unit = {
      super.update(SCE(ownerID, value))
    }


    class SubIndexBaseImpl[FK, K](val rawIndex: SubRawIndexBaseImpl[FK, K], ownerID: Long) {
      def subHostCollection = subHost

      def apply(range: Range[K], forward: Boolean)(implicit db: DBSessionQueryable): Cursor[V] = {
        rawIndex.apply(ownerID, range, forward)
      }
    }

    abstract class SubRawIndexBaseImpl[FK: Packer, K: Packer](name: String) extends IndexBaseImpl[FK](name) {
      val cursorKeyRangePacker = Packer.of[(Int, Long, RangeBound[K])]

      def apply(ownerID: Long, range: Range[K], forward: Boolean)(implicit db: DBSessionQueryable): Cursor[V] = {
        val r = range.map(r => (this.getIndexID, ownerID, r))(cursorKeyRangePacker)
        new CursorImpl[Long](r, forward) map { id =>
          subHost(id).get.v
        }
      }
    }

    def defUniqueIndex[K: Packer : TypeTag](ownerID: Long, indexName: String, keyGetter: V => K): UniqueIndex[K, V] = {
      type FullKey = (Int, Long, K)
      indexes.synchronized[UniqueIndex[K, V]] {
        val rawIndex = this.indexes.getOrElseUpdate(indexName,
          new SubRawIndexBaseImpl[FullKey, K](indexName) {

            override def v2fk(value: SCE[V])(implicit db: DBSessionQueryable) = (this.getIndexID, value.ownerID, keyGetter(value.v))
          }
        ).asInstanceOf[SubRawIndexBaseImpl[FullKey, K]]
        new SubIndexBaseImpl[FullKey, K](rawIndex, ownerID) with UniqueIndex[K, V] {

          override def update(value: V)(implicit db: DBSessionUpdatable): Unit = {
            subHost.update(SCE(ownerID, value))
          }

          override def delete(key: K)(implicit db: DBSessionUpdatable): Unit = {
            rawIndex.deleteByFullKey((rawIndex.getIndexID, ownerID, key))
          }

          override def apply(key: K)(implicit db: DBSessionQueryable): Option[V] = {
            rawIndex.getByFullKey(rawIndex.getIndexID, ownerID, key).map(_.v)
          }

        }
      }
    }

    final def defIndex[K: Packer : TypeTag](ownerID: Long, indexName: String, keyGetter: V => K): Index[K, V] = {
      type FullKey = (Int, Long, K, Long)
      indexes.synchronized[Index[K, V]] {
        val rawIndex = this.indexes.getOrElseUpdate(indexName,
          new SubRawIndexBaseImpl[FullKey, K](indexName) {
            override def v2fk(value: SCE[V])(implicit db: DBSessionQueryable) = (this.getIndexID, value.ownerID, keyGetter(value.v), value.id)
          }
        ).asInstanceOf[SubRawIndexBaseImpl[FullKey, K]]
        new SubIndexBaseImpl[FullKey, K](rawIndex, ownerID) with Index[K, V]
      }
    }

    val sce2v = Some((s: SCE[V]) => s.v)

    @inline final def addSubListener(listener: EntityCollectionListener[V]) = {
      this.regListener(listener, sce2v, reg = true)
    }

    @inline final def removeSubListener(listener: EntityCollectionListener[V]) = {
      this.regListener(listener, sce2v, reg = false)
    }
  }

}


abstract class DataView[K: Packer, V: Packer] {
  private var viewID = 0

  private def getViewID(implicit db: DBSessionQueryable) = {
    if (this.viewID == 0) {
      this.viewID = ???
    }
    this.viewID
  }

  private type FullKey = (Int, K)
  private implicit val fullKeyPacker = implicitly[Packer[(Int, K)]]

  def name: String

  def apply(key: K)(implicit db: DBSessionQueryable): Option[V] = db.get[FullKey, V](this.getViewID, key)
  
  protected sealed trait Tracer[BUF <: AnyRef, E <: Entity] extends EntityCollectionListener[E] {
    tracer =>

    def extract(entity: E, buf: BUF)

    def isChange(oldBuf: BUF, newBuf: BUF): Boolean

    private[common] val bufCreator: () => BUF

    private[common] def traceUp(oldEntity: E, oldBuf: BUF, newEntity: E, newBuf: BUF, excludeSubTracer: Tracer[BUF, _])(implicit db: DBSessionUpdatable): Unit

    protected abstract class SubRef[R <: Entity](target: EntityCollection[R], refGetter: E => Ref[R], refIndex: IndexBase[_ >: Ref[R]
      with Product1[Ref[R]]
      with Product2[Ref[R], Nothing]
      with Product3[Ref[R], Nothing, Nothing]
      with Product4[Ref[R], Nothing, Nothing, Nothing]
      with Product5[Ref[R], Nothing, Nothing, Nothing, Nothing]
      with Product6[Ref[R], Nothing, Nothing, Nothing, Nothing, Nothing]
      with Product7[Ref[R], Nothing, Nothing, Nothing, Nothing, Nothing, Nothing]
      , E]) extends Tracer[BUF, R] {

      target.addListener(this)
      tracer.addSub(this)

      private val index = refIndex.asInstanceOf[MayBeReverseIndex[E]]


      import com.weez.mercury.common.EntityCollections._

      private val targetHost = target match {
        case x: RootCollection[R] => x.impl
        case _ => null.asInstanceOf[HostCollectionImpl[R]]
      }
      private val targetSub = target match {
        case x: SubCollection[R] => x.host
        case _ => null.asInstanceOf[SubHostCollectionImpl[R]]
      }

      override final def onEntityInsert(newEntity: R)(implicit db: DBSessionUpdatable): Unit = {
        //doNothing
      }

      override final def onEntityDelete(oldEntity: R)(implicit db: DBSessionUpdatable): Unit = {
        val oldBuf = bufCreator()
        this.extract(oldEntity, oldBuf)
        this.traceUp(oldEntity, oldBuf, null.asInstanceOf[R], null.asInstanceOf[BUF], null)
      }

      override final def onEntityUpdate(oldEntity: R, newEntity: R)(implicit db: DBSessionUpdatable): Unit = {
        val oldBuf = bufCreator()
        val newBuf = bufCreator()
        this.extract(oldEntity, oldBuf)
        this.extract(newEntity, newBuf)
        this.traceUp(oldEntity, oldBuf, newEntity, newBuf, null)
      }

      /**
       * REF or NULL
       */
      @inline final private[common] def getRef(entity: E)(implicit db: DBSessionUpdatable): R = {
        if (entity ne null) {
          val ref = refGetter(entity)
          if (ref ne null) {
            val id = ref.id
            if (id != 0l) {
              if (targetHost ne null) {
                return targetHost(id).get
              } else {
                return targetSub(id).get.v
              }
            }
          }
        }
        null.asInstanceOf[R]
      }

      @inline final def traceDown(oldEntity: E, oldBuf: BUF, newEntity: E, newBuf: BUF)(implicit db: DBSessionUpdatable): Unit = {
        val or = getRef(oldEntity)
        val nr = getRef(newEntity)
        if (oldBuf ne null) {
          this.extract(or, oldBuf)
        }
        if (newBuf ne null) {
          this.extract(nr, newBuf)
        }
        traceDownSub(or, oldBuf, nr, newBuf, null)
      }

      @inline final private[common] override def traceUp(oldEntity: R, oldBuf: BUF, newEntity: R, newBuf: BUF, excludeSubTracer: Tracer[BUF, _])(implicit db: DBSessionUpdatable): Unit = {
        traceDownSub(oldEntity, oldBuf, newEntity, newBuf, excludeSubTracer)
        for (u <- index.scanReverse(oldEntity.newRef())) {
          tracer.extract(u, oldBuf)
          if (newBuf != null) {
            tracer.extract(u, newBuf)
          }
          tracer.traceUp(u, oldBuf, u, newBuf, this)
        }
      }

      override private[common] val bufCreator = tracer.bufCreator
    }


    private var subRefs: Array[SubRef[_]] = null

    @inline private[common] final def addSub(sub: SubRef[_]): Unit = {
      if (subRefs eq null) {
        subRefs = Array[SubRef[_]](sub)
      } else {
        val l = subRefs.length
        val newSubRefs = new Array[SubRef[_]](l + 1)
        System.arraycopy(subRefs, 0, newSubRefs, 0, l)
        newSubRefs(l) = sub
        subRefs = newSubRefs
      }
    }

    @inline private[common] final def traceDownSub(oldEntity: E, oldBuf: BUF, newEntity: E, newBuf: BUF, excludeSubTracer: Tracer[BUF, _])(implicit db: DBSessionUpdatable): Unit = {
      if (subRefs != null) {
        var i = 0
        val l = subRefs.length
        while (i < l) {
          val st = subRefs(i)
          if (st ne excludeSubTracer) {
            st.traceDown(oldEntity, oldBuf, newEntity, newBuf)
          }
          i += 1
        }
      }
    }
  }

  protected abstract class DataViewMeta[BUF <: AnyRef, ROOT <: Entity : TypeTag](root: EntityCollection[ROOT]) extends Tracer[BUF, ROOT] {

    def createBuf(): BUF

    private[common] val bufCreator = createBuf _

    def extractKV(buf: BUF): Seq[(K, V)]

    def extractK(buf: BUF): Seq[V] = extractKV(buf).map(_._2)


    override final def onEntityInsert(newEntity: ROOT)(implicit db: DBSessionUpdatable): Unit = {
      val newBuf = createBuf()
      this.extract(newEntity, newBuf)
      this.traceUp(null.asInstanceOf[ROOT], null.asInstanceOf[BUF], newEntity, newBuf, null)
    }

    override final def onEntityDelete(oldEntity: ROOT)(implicit db: DBSessionUpdatable): Unit = {
      val oldBuf = createBuf()
      this.extract(oldEntity, oldBuf)
      this.traceUp(oldEntity, oldBuf, null.asInstanceOf[ROOT], null.asInstanceOf[BUF], null)
    }

    override final def onEntityUpdate(oldEntity: ROOT, newEntity: ROOT)(implicit db: DBSessionUpdatable): Unit = {
      val oldBuf = createBuf()
      val newBuf = createBuf()
      this.extract(oldEntity, oldBuf)
      this.extract(newEntity, newBuf)
      this.traceUp(oldEntity, oldBuf, newEntity, newBuf, null)
    }

    private[common] def traceUp(oldEntity: ROOT, oldBuf: BUF, newEntity: ROOT, newBuf: BUF, excludeSubTracer: Tracer[BUF, _])(implicit db: DBSessionUpdatable): Unit = {
      traceDownSub(oldEntity, oldBuf, newEntity, newBuf, excludeSubTracer)
      if ((oldBuf ne null) && (newBuf ne null)) {
        val oldKV = this.extractKV(oldBuf)
        val newKV = this.extractKV(newBuf)

      } else if (newBuf ne null) {
        this.extractKV(newBuf).foreach { kv =>
          db.put(kv._1, kv._2)
        }
      } else {
        this.extractK(oldBuf).foreach { k =>
          db.del(k)
        }
      }
    }

    root.addListener(this)
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
