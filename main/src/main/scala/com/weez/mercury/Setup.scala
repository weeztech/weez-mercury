package com.weez.mercury

import akka.actor._
import com.typesafe.config.ConfigFactory
import com.weez.mercury.common._

object Setup {
  def main(args: Array[String]): Unit = {
    val system = ActorSystem("mercury")
    system.actorOf(Props(classOf[SetupActor]))
  }

  class SetupActor extends Actor with ActorLogging {
    override def preStart() = {
      self ! Unit
    }

    def receive = {
      case _ =>
        val dbPath = context.system.settings.config.getString("weez.database")
        val backend = RocksDBBackend
        backend.delete(dbPath)
        val db = backend.createNew(Util.resolvePath(dbPath))
        implicit val dbSession = db.createSession()

        dbSession.close()
        db.close()
        context.system.shutdown()
    }
  }

}
