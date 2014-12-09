package com.weez.mercury.common

import scala.language.implicitConversions
import scala.language.existentials
import scala.concurrent._
import scala.util._
import scala.util.control.NoStackTrace
import spray.json._
import akka.actor._
import DB.driver.simple._

class ServiceManager extends Actor with RemoteCallDispatcher {
  val serviceCalls = Map[String, RemoteCall](
    LoginService
  )

  val tables = Seq(
    com.weez.mercury.common.Staffs,
    com.weez.mercury.product.Products,
    com.weez.mercury.product.ProductPrices
  )

  private val db = Database.forConfig("weez-mercury.database", context.system.settings.config)
  private val simpleWorker = context.system.actorOf(Props(new QueryWorkerActor), "simple-worker")
  private val queryWorker = context.system.actorOf(Props(new QueryWorkerActor), "query-worker")
  private val persistWorker = context.system.actorOf(Props(new PersistWorkerActor), "persist-worker")

  private implicit def call2tuple(rc: RemoteCall): (String, RemoteCall) = {
    rc.getClass.getName -> rc
  }

  import ServiceCommand._

  def receive = {
    case WebRequest(sid, clazz, in, p) =>
      import ErrorCode._
      try {
        val req = in.fields.getOrElse("request", InvalidRequest.raise)
        val rc = serviceCalls.getOrElse(clazz, NotAcceptable.raise)
        rc.dispatch(this, req, p)
      } catch {
        case ex: Throwable => Future.failed(ex)
      }
    case Setup =>
      db.withTransaction { implicit session =>
        tables foreach { x =>
          try {
            x.ddl.drop
          } catch {
            case _: Throwable =>
          }
          x.ddl.create
        }

      }
  }

  def postSimpleTask(f: SimpleContext => JsValue, promise: Promise[JsValue]) = {
    simpleWorker ! SimpleTask(f, promise)
  }

  def postQueryTask(f: QueryContext => JsValue, promise: Promise[JsValue]) = {
    queryWorker ! QueryTask(f, promise)
  }

  def postPersistTask(f: PersistContext => JsValue, promise: Promise[JsValue]) = {
    persistWorker ! PersistTask(f, promise)
  }

  class SimpleWorkerActor extends Actor {
    private val requestCountLimit = context.system.settings.config.getInt("weez-mercury.simple-request-count-limit")
    private var workerCount = 0

    def receive = {
      case SimpleTask(f, p) =>
        if (workerCount < requestCountLimit) {
          import context.dispatcher
          Future {
            p.complete(Try(f(new SimpleContext {})))
            self ! Done
          }
        } else {
          p.failure(ErrorCode.Reject)
        }
      case Done =>
        workerCount -= 1
    }
  }

  class QueryWorkerActor extends Actor {
    private val maxWorkerCount = context.system.settings.config.getInt("weez-mercury.query-worker-count-max")
    private val minWorkerCount = context.system.settings.config.getInt("weez-mercury.query-worker-count-min")
    private val requestCountLimit = context.system.settings.config.getInt("weez-mercury.query-request-count-limit")

    private val queue = scala.collection.mutable.Queue[QueryTask]()
    private val idle = scala.collection.mutable.Queue[ActorRef]()
    private var workerCount = 0

    def receive = {
      case x: QueryTask =>
        if (idle.nonEmpty) {
          idle.dequeue() ! x
        } else if (workerCount < maxWorkerCount) {
          val worker = context.actorOf(Props(new WorkerActor))
          worker ! x
          workerCount += 1
        } else if (queue.size + workerCount < requestCountLimit) {
          queue.enqueue(x)
        } else {
          x.p.failure(ErrorCode.Reject)
        }
      case Done =>
        if (queue.nonEmpty) {
          sender ! queue.dequeue()
        } else if (workerCount > minWorkerCount) {
          context.stop(sender)
          workerCount -= 1
        } else {
          idle.enqueue(sender)
        }
    }

    class WorkerActor extends Actor {
      private val session = db.createSession()

      override def postStop(): Unit = {
        session.close()
      }

      def receive = {
        case QueryTask(f, p) =>
          p.complete(Try {
            val c = new QueryContext {
              val dbSession = session
            }
            session.withTransaction {
              f(c)
            }
          })
          sender ! Done
      }
    }

  }

  class PersistWorkerActor extends Actor {
    private val session = db.createSession()

    override def postStop(): Unit = {
      session.close()
    }

    def receive = {
      case PersistTask(f, p) =>
        p.complete(Try {
          val c = new PersistContext {
            val dbSession = session
          }
          session.withTransaction {
            f(c)
          }
        })
    }
  }

  case object Done

  case class SimpleTask(f: SimpleContext => JsValue, p: Promise[JsValue])

  case class QueryTask(f: QueryContext => JsValue, p: Promise[JsValue])

  case class PersistTask(f: PersistContext => JsValue, p: Promise[JsValue])

}

case class WebRequest(sid: String, clazz: String, in: JsObject, promise: Promise[JsValue])

object ServiceCommand extends Enumeration {
  val Setup = Value
}
