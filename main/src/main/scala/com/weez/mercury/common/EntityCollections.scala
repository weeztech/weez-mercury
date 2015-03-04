package com.weez.mercury.common

import java.util.concurrent.atomic.AtomicReference

import scala.collection.mutable
import com.weez.mercury.imports.packable
import com.weez.mercury.macros.TuplePrefixed


private[common] object EntityCollections {


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


  @inline final def findHost[E <: Entity](collectionID: Int)(implicit db: DBSessionQueryable): RootCollectionImpl[E] = {
    findDBObject(collectionID).asInstanceOf[RootCollectionImpl[E]]
  }

  @inline
  final def getHostO[E <: Entity](collectionID: Int)(implicit db: DBSessionQueryable): Option[RootCollectionImpl[E]] = {
    Option(findHost[E](collectionID))
  }

  @inline
  final def getHost[E <: Entity](collectionID: Int)(implicit db: DBSessionQueryable): RootCollectionImpl[E] = {
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

    @inline final def scan[V <: Entity](id: Long, host: RootCollectionImpl[V])(implicit db: DBSessionQueryable): Cursor[V] = {
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


    @inline final def scan[V <: Entity](word: String, host: RootCollectionImpl[V])(implicit db: DBSessionQueryable): Cursor[V] = {
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
          findDBObject(collectionIDOf(rid)).asInstanceOf[RootCollectionImpl[_]].setHasReverseExtracts()
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