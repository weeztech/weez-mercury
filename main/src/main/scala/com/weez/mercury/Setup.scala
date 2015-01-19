package com.weez.mercury

import akka.actor._
import com.typesafe.config.ConfigFactory
import com.weez.mercury.common._

object Setup {
  def main(args: Array[String]): Unit = {
    val system = ActorSystem("mercury")
    val setup = system.actorOf(Props(classOf[SetupActor]), "setup")
    setup ! DeleteDB :: NewDB :: SetupDBTypes :: CloseDB :: Shutdown :: Nil
  }

  class SetupActor extends Actor with ActorLogging {
    val backend = RocksDBBackend
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
        val dbtypes = new DBTypeCollector
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

          dbtypes.resolvedTypes.values foreach {
            case EntityMeta(_, name, cols) =>
              EntityMetaCollection.update(EntityMeta(dbc.newEntityId(), name, cols))
            case CollectionMeta(_, name, vtype, indexes, root, _) =>
              CollectionMetaCollection.update(
                CollectionMeta(dbc.newEntityId(), name, vtype,
                  indexes map { i => IndexMeta(i.name, i.key, i.unique, newPrefix)},
                  root, newPrefix))
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
