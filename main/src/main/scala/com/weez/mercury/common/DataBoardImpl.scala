package com.weez.mercury.common

import EntityCollections._

private final class DataBoardImpl[D] extends ExtractorSource[Seq[D]] with ExtractorTarget[Seq[D]] {
  def indirectExtract(tracer: RefTracer, host: RootCollectionImpl[Entity], next: Extractor[_, _])(implicit db: DBSessionUpdatable): Boolean = {
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

final class DataBoard2DataViewExtractor[SD, TK, TV](source: DataBoardImpl[SD],
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
