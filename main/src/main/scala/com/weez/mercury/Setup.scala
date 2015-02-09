package com.weez.mercury

import akka.actor._
import com.weez.mercury.common._

object Setup {
  def main(args: Array[String]): Unit = {
    val system = ActorSystem("mercury")
    val g = new GlobalSettings {
      val types = ClassFinder.collectTypes(system.log).map(tp => tp._1 -> tp._2.toSeq).toMap
      val config = system.settings.config.getConfig("weez-mercury")
      val devmode = config.getBoolean("devmode")
    }
    val setup = system.actorOf(Props(classOf[SetupActor], g), "setup")
    setup ! DeleteDB :: NewDB :: SetupDBTypes :: CloseDB :: Shutdown :: Nil
  }

  class SetupActor(g: GlobalSettings) extends Actor with ActorLogging {
    val backend = new RocksDBDatabaseFactory(g)
    val dbPath = context.system.settings.config.getString("weez-mercury.database")
    var db: Database = _

    def receive = {
      case x: List[_] =>
        val pf = receive
        x foreach { m => pf.applyOrElse(m, unhandled)}
      case DeleteDB =>
        if (db != null) {
          db.close()
          db = null
        }
        backend.delete(dbPath)
      case NewDB =>
        db = backend.createNew(Util.resolvePath(dbPath))
      case CloseDB =>
        db.close()
      case Shutdown =>
        context.system.shutdown()
      case SetupDBTypes =>
        if (db == null) throw new IllegalStateException()
        val dbtypes = new DBTypeCollector(g.types)
        dbtypes.collectDBTypes(log)
        val dbSession = db.createSession()
        val dbFactory = new DBSessionFactory(dbSession)

        withTrans(dbSession, dbFactory) { implicit dbc =>
          dbc.put(dbFactory.KEY_OBJECT_ID_COUNTER, 0L)
        }
        withTrans(dbSession, dbFactory) { implicit dbc =>
          var counter = 10

          def newPrefix = {
            counter += 1
            counter
          }

          import DBType._
          MetaCollection.insert(DBMetas.metaCollectionMeta)
          dbtypes.resolvedMetas.values foreach {
            case x: CollectionMeta =>
              if (x.name != DBMetas.metaCollectionMeta.name)
                MetaCollection.insert(x.copy(prefix = newPrefix, indexes = x.indexes.map(_.copy(prefix = newPrefix))))
            case x: DataViewMeta =>
              MetaCollection.insert(x.copy(prefix = newPrefix))
            case x =>
              MetaCollection.insert(x)
          }
          dbc.put(dbFactory.KEY_PREFIX_ID_COUNTER, newPrefix)
        }
        dbSession.close()
    }

    def withTrans(dbSession: DBSession, dbFactory: DBSessionFactory)(f: DBSessionUpdatable => Unit) = {
      val trans = dbSession.newTransaction(log)
      try {
        f(dbFactory.create(trans, log))
        trans.commit()
      } finally {
        trans.close()
      }
    }
  }

  sealed trait Command

  case object DeleteDB extends Command

  case object NewDB extends Command

  case object CloseDB extends Command

  case object SetupDBTypes extends Command

  case object Shutdown extends Command

}
