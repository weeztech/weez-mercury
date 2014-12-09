package com.weez.mercury.common

import akka.actor._
import DB.driver.simple._
import spray.json.JsValue

import scala.concurrent.Promise
import scala.slick.jdbc.JdbcBackend

class WorkerActor extends Actor {
  val cc = context.system.dispatchers.lookup(context.system.settings.config.getString("weez-mercury.simple-worker-dispatcher"))

  def receive = {
    case x: RequestTask =>
      import ContextBinding._
      x.rc.contextBinding match {
        case SimpleContextBinding =>
        case QueryContextBinding =>
        case PersistContextBinding =>
      }

      try {
        val resp = rc.internalCall(this, req)
        p.success(resp)
      } catch {
        case ex: Throwable =>
          p.failure(ex)
      }
  }
}

case class RequestTask(rc: RemoteCall, req: JsValue, p: Promise[JsValue])


class QueryWorkerActor extends Actor {
  def receive = {
    case x: RequestTask =>
      val worker = context.actorOf(Props(new WorkerActor))
      worker ! x
  }

  class WorkerActor extends Actor {
    private var session: JdbcBackend#Session = null

    override def preStart() = {
      val db = Database.forConfig("weez-mercury.database-connection", context.system.settings.config)
      session = db.createSession()
    }

    override def postStop(): Unit = {
      session.close()
      session = null
    }

    def receive = {
      case x: RequestTask =>
    }
  }

}

class PersistWorkerActor extends Actor {
  private var session: JdbcBackend#Session = null

  override def preStart() = {
    val db = Database.forConfig("weez-mercury.database-connection", context.system.settings.config)
    session = db.createSession()
  }

  override def postStop(): Unit = {
    session.close()
    session = null
  }

  def receive = {
    case RequestTask(sc: ServiceCall[_, _, _], req, p) =>
      session.withTransaction {
        val c = new PersistContext {
          val dbsession = session
        }
        sc.asInstanceOf[ServiceCall[PersistContext, _, _]].call(c, req)
      }
  }
}