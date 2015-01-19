package com.weez.mercury.common

class DBSessionFactory(dbSession: DBSession) {

  import akka.event.LoggingAdapter

  private val metaCache = scala.collection.mutable.Map(
    EntityMetaCollection.name -> DBType.CollectionMeta(0,
      EntityMetaCollection.name,
      DBType.Ref("entity-meta"),
      DBType.IndexMeta("by-name", DBType.String, true, 0) :: Nil,
      true, 0),
    CollectionMetaCollection.name -> DBType.CollectionMeta(0,
      CollectionMetaCollection.name,
      DBType.Ref("collection-meta"),
      DBType.IndexMeta("by-name", DBType.String, true, 0) :: Nil,
      true, 0))

  val KEY_OBJECT_ID_COUNTER = "object-id-counter"
  val KEY_PREFIX_ID_COUNTER = "prefix-id-counter"

  private var allocIdLimit = 0L
  private var currentId = allocIdLimit
  private val allocIdBatch = 1024

  def allocId(log: LoggingAdapter) = {
    KEY_OBJECT_ID_COUNTER.synchronized {
      if (allocIdLimit == 0) {
        dbSession.withTransaction(log) { trans =>
          allocIdLimit = Packer.unpack[Long](trans.get(Packer.pack(KEY_OBJECT_ID_COUNTER)))
        }
      }
      if (currentId == allocIdLimit) {
        allocIdLimit += allocIdBatch
        dbSession.withTransaction(log) { trans =>
          trans.put(Packer.pack(KEY_OBJECT_ID_COUNTER), Packer.pack(allocIdLimit))
        }
      }
      currentId += 1
      currentId
    }
  }

  def create(trans: DBTransaction, log: LoggingAdapter): DBSessionUpdatable = new DBSessionImpl(trans, log)

  def withTransaction[T](dbSession: DBSession, log: LoggingAdapter)(f: DBSessionUpdatable => T): T = {
    dbSession.withTransaction(log) { trans =>
      f(create(trans, log))
    }
  }

  private final class DBSessionImpl(trans: DBTransaction, log: LoggingAdapter) extends DBSessionUpdatable {
    def get[K, V](key: K)(implicit pk: Packer[K], pv: Packer[V]) = {
      val arr = trans.get(pk(key))
      if (arr == null) None else Some(pv.unapply(arr))
    }

    @inline def exists[K](key: K)(implicit pk: Packer[K]) = trans.exists(pk(key))

    @inline def newCursor() = trans.newCursor

    def getRootCollectionMeta(name: String)(implicit db: DBSessionQueryable) = {
      metaCache.get(name) match {
        case Some(x) => x
        case None =>
          CollectionMetaCollection.byName(name) match {
            case Some(x) => x
            case None => throw new Error("meta of root-collection not found")
          }
      }
    }

    @inline def newEntityId() = allocId(log)

    @inline def put[K, V](key: K, value: V)(implicit pk: Packer[K], pv: Packer[V]) = trans.put(pk(key), pv(value))

    @inline def del[K](key: K)(implicit pk: Packer[K]) = trans.del(pk(key))
  }

}