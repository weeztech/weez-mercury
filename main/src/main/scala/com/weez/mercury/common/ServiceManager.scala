package com.weez.mercury.common

import scala.language.implicitConversions
import scala.language.existentials
import scala.concurrent._
import scala.util._
import scala.util.control.NoStackTrace
import spray.json._
import akka.actor._
import DB.driver.simple._

class ServiceManager(system: ActorSystem) extends RemoteCallDispatcher {
  val serviceCalls = Map[String, RemoteCall](
    LoginService
  )

  private val db = Database.forConfig("weez-mercury.database-connection", system.settings.config)
  private val simpleWorker = system.actorOf(Props(new QueryWorkerActor), "simple-worker")
  private val queryWorker = system.actorOf(Props(new QueryWorkerActor), "query-worker")
  private val persistWorker = system.actorOf(Props(new PersistWorkerActor), "persist-worker")

  private implicit def call2tuple(rc: RemoteCall): (String, RemoteCall) = {
    rc.getClass.getName -> rc
  }

  def call(clazz: String, in: JsObject): Future[JsValue] = {
    import ErrorCode._
    try {
      val req = in.fields.getOrElse("value", throw new ProcessException(InvalidRequest))
      val rc = serviceCalls.getOrElse(clazz, throw new ProcessException(NotAcceptable))
      rc.dispatch(this, req)
    } catch {
      case x: ProcessException =>
        Future.successful(JsObject("error" -> JsNumber(x.err.code)))
    }
  }

  def postSimpleTask(f: SimpleContext => JsValue): Future[JsValue] = {
    val p = Promise[JsValue]
    simpleWorker ! SimpleTask(f, p)
    p.future
  }

  def postQueryTask(f: QueryContext => JsValue): Future[JsValue] = {
    val p = Promise[JsValue]
    queryWorker ! QueryTask(f, p)
    p.future
  }

  def postPersistTask(f: PersistContext => JsValue): Future[JsValue] = {
    val p = Promise[JsValue]
    persistWorker ! PersistTask(f, p)
    p.future
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
          p.failure(ExceedLimitException)
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
        } else if (queue.size + workerCount <= requestCountLimit) {
          queue.enqueue(x)
        } else {
          x.p.failure(ExceedLimitException)
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
              val dbsession = session
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
            val dbsession = session
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

  object ExceedLimitException extends NoStackTrace

  class ProcessException(val err: ErrorCode) extends Exception(err.message)

}

