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
          allocIdLimit = trans.get[String, Long](KEY_OBJECT_ID_COUNTER).get
        }
      }
      if (currentId == allocIdLimit) {
        allocIdLimit += allocIdBatch
        dbSession.withTransaction(log) { trans =>
          trans.put(KEY_OBJECT_ID_COUNTER, allocIdLimit)
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
    @inline def get[K: Packer, V: Packer](key: K) = trans.get[K, V](key)

    @inline def exists[K: Packer](key: K) = trans.exists(key)

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

    @inline def put[K: Packer, V: Packer](key: K, value: V) = trans.put(key, value)

    @inline def del[K: Packer](key: K) = trans.del(key)
  }

}