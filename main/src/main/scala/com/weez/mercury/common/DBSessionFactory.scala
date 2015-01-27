package com.weez.mercury.common

class DBSessionFactory(dbSession: DBSession) {

  import akka.event.LoggingAdapter

  private val metaCache = scala.collection.mutable.Map(
    EntityMetaCollection.name -> DBType.CollectionMeta(0,
      EntityMetaCollection.name,
      DBType.EntityRef("entity-meta"),
      DBType.IndexMeta("by-name", DBType.String, unique = true, 0) :: Nil,
      isRoot = true, 1),
    CollectionMetaCollection.name -> DBType.CollectionMeta(0,
      CollectionMetaCollection.name,
      DBType.EntityRef("collection-meta"),
      DBType.IndexMeta("by-name", DBType.String, unique = true, 0) :: Nil,
      isRoot = true, 2))

  val KEY_OBJECT_ID_COUNTER = "object-id-counter"
  val KEY_PREFIX_ID_COUNTER = "prefix-id-counter"

  private var allocIdLimit = 0L
  private var currentId = allocIdLimit
  private val allocIdBatch = 1024

  def allocId(log: LoggingAdapter) = {
    KEY_OBJECT_ID_COUNTER.synchronized {
      if (currentId == allocIdLimit) {
        val trans = dbSession.newTransaction(log)
        try {
          if (allocIdLimit == 0)
            allocIdLimit = Packer.unpack[Long](trans.get(Packer.pack(KEY_OBJECT_ID_COUNTER)))
          allocIdLimit += allocIdBatch
          trans.put(Packer.pack(KEY_OBJECT_ID_COUNTER), Packer.pack(allocIdLimit))
          trans.commit()
        } finally {
          trans.close()
        }
      }
      currentId += 1
      currentId
    }
  }

  def create(trans: DBTransaction, log: LoggingAdapter): DBSessionUpdatable = new DBSessionImpl(trans, log)

  def withTransaction[T](dbSession: DBSession, log: LoggingAdapter)(f: DBSessionUpdatable => T): T = {
    val trans = dbSession.newTransaction(log)
    try {
      val ret = f(create(trans, log))
      trans.commit()
      ret
    } finally {
      trans.close()
    }
  }

  private final class DBSessionImpl(trans: DBTransaction, log: LoggingAdapter) extends DBSessionUpdatable {
    def get[K, V](key: K)(implicit pk: Packer[K], pv: Packer[V]) = {
      val arr = trans.get(pk(key))
      if (arr == null) None else Some(pv.unapply(arr))
    }

    @inline def exists[K](key: K)(implicit pk: Packer[K]) = trans.exists(pk(key))

    @inline def newCursor() = trans.newCursor()

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