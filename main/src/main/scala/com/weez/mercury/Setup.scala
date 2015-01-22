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
        val dbtypes = new DBTypeCollector(g)
        dbtypes.collectDBTypes(log)
        val dbSession = db.createSession()
        val dbFactory = new DBSessionFactory(dbSession)
        dbFactory.withTransaction(dbSession, log) { implicit dbc =>
          var prefixCounter = 0

          def newPrefix = {
            prefixCounter += 1
            prefixCounter
          }

          import DBType._

          dbtypes.resolvedDBTypes.values foreach {
            case x: EntityMeta =>
              EntityMetaCollection.update(EntityMeta(dbc.newEntityId(), x.name, x.columns, x.parents, x.isTopLevel, x.isAbstract))
            case x: CollectionMeta =>
              CollectionMetaCollection.update(
                CollectionMeta(dbc.newEntityId(),
                  x.name, x.valueType,
                  x.indexes map { i => IndexMeta(i.name, i.key, i.unique, newPrefix)},
                  x.isRoot, newPrefix))
            case _ => throw new IllegalStateException()
          }
          dbc.put(dbFactory.KEY_PREFIX_ID_COUNTER, prefixCounter)
        }
        dbSession.close()
    }
  }

  sealed trait Command

  case object DeleteDB extends Command

  case object NewDB extends Command

  case object CloseDB extends Command

  case object SetupDBTypes extends Command

  case object Shutdown extends Command

}
