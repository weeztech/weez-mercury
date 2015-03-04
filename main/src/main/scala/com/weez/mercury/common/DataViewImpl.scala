package com.weez.mercury.common

import com.weez.mercury.imports.packable
import com.weez.mercury.macros.TuplePrefixed
import EntityCollections._

@packable
private[common] case class DataViewImplKey[K](prefix: Int, key: K)

private class DataViewImpl[DK, DV](val name: String)
                                  (implicit fullKeyPacker: Packer[DataViewImplKey[DK]], vPacker: Packer[DV])
  extends DBObject with ExtractorTarget[Map[DK, DV]] with IndirectExtract {

  @inline final def put(fk: DataViewImplKey[DK], v: DV)(implicit db: DBSessionUpdatable): Unit = db.put(fk, v)

  @inline final def del(fk: DataViewImplKey[DK])(implicit db: DBSessionUpdatable): Unit = db.del(fk)

  @inline final def get(fk: DataViewImplKey[DK])(implicit db: DBSessionQueryable): Option[DV] = db.get(fk)(fullKeyPacker, vPacker)

  @inline final def put(k: DK, v: DV)(implicit db: DBSessionUpdatable): Unit = put(DataViewImplKey(getDataViewID, k), v)

  @inline final def del(k: DK)(implicit db: DBSessionUpdatable): Unit = del(DataViewImplKey(getDataViewID, k))

  @inline final def get(k: DK)(implicit db: DBSessionQueryable): Option[DV] = get(DataViewImplKey(getDataViewID, k))

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

  final def scan(dbRange: DBRange, forward: Boolean)(implicit db: DBSessionQueryable): Cursor[KeyValue[DK, DV]] = {
    new RawKVCursor[KeyValue[DK, DV]](dbRange, forward) {
      override def buildValue() = {
        val rk = fullKeyPacker.unapply(rawKey)
        val rv = vPacker.unapply(rawValue)
        KeyValue[DK, DV](rk.key, rv)
      }
    }
  }

  final def apply(dbRange: DBRange, forward: Boolean)(implicit db: DBSessionQueryable): Cursor[KeyValue[DK, DV]] = {
    scan(dbRange,forward)
  }

  final def apply()(implicit db: DBSessionQueryable): Cursor[KeyValue[DK, DV]] = {
    import Range._
    val prefix = Tuple1(getDataViewID)
    scan(prefix +-+ prefix, forward = true)
  }


  final def contains(key: DK)(implicit db: DBSessionQueryable): Boolean = db.exists(DataViewImplKey(getDataViewID, key))

  final def apply[P: Packer](range: Range[P], forward: Boolean = true)(implicit db: DBSessionQueryable, canUse: TuplePrefixed[DK, P]): Cursor[KeyValue[DK, DV]] = {
    val dvID = getDataViewID
    val r = if (canUse.shouldTuple) range.map(r => (dvID, Tuple1(r))) else range.map(r => (dvID, r))
    scan(r, forward)
  }
}

private class MergeDataViewImpl[DK, DV <: CanMerge[DV]](name: String, noDBMerge: Boolean)(implicit fullKeyPacker: Packer[DataViewImplKey[DK]], vPacker: Packer[DV])
  extends DataViewImpl[DK, DV](name) {

  override def merge(v1: DV, v2: DV): DV = v1 + v2

  @inline
  private final def putOld(fk: DataViewImplKey[DK], v: DV)(implicit db: DBSessionUpdatable): Unit = {
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
  private final def putNew(fk: DataViewImplKey[DK], v: DV)(implicit db: DBSessionUpdatable): Unit = {
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
        putOld(DataViewImplKey(viewID, k), v)
      }
    }
  }

  override def targetExtractNew(newKV: Map[DK, DV], tracer: RefTracer)(implicit db: DBSessionUpdatable): Unit = {
    if (noDBMerge) {
      super.targetExtractNew(newKV, tracer)
    } else {
      val viewID = getDataViewID
      for ((k, v) <- newKV) {
        putNew(DataViewImplKey(viewID, k), v)
      }
    }
  }

  override def targetExtractBoth(oldKV: Map[DK, DV], newKV: Map[DK, DV], tracer: RefTracer)(implicit db: DBSessionUpdatable): Unit = {
    if (noDBMerge) {
      super.targetExtractBoth(oldKV, newKV, tracer)
    } else {
      val viewID = getDataViewID
      for ((k, v) <- newKV) {
        val fk = DataViewImplKey(viewID, k)
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
          putOld(DataViewImplKey(viewID, k), v)
        }
      }
    }
  }
}

private object DataViewImpl {

  import scala.reflect.runtime.universe._

  def apply[K: Packer, V: TypeTag](stub: AnyRef)(implicit vPacker: Packer[V]): DataViewImpl[K, V] = {
    val fkp = Packer.of[DataViewImplKey[K]]
    def newMDV(name: String, noDBMerge: Boolean) = {
      trait CC extends CanMerge[CC]
      new MergeDataViewImpl[K, CC](name, noDBMerge)(fkp, vPacker.asInstanceOf[Packer[CC]]).asInstanceOf[DataViewImpl[K, V]]
    }
    newDBObject(stub) { name =>
      if (typeOf[V] <:< typeOf[CanMerge[_]]) {
        newMDV(name, stub.isInstanceOf[NoMergeInDB])
      } else {
        new DataViewImpl[K, V](name)(fkp, vPacker)
      }
    }
  }
}