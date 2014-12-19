package com.weez.mercury.common

import scala.language.implicitConversions
import scala.language.existentials
import scala.concurrent._
import scala.util._
import spray.json._
import akka.actor._
import DB.driver.simple._

class ServiceManager extends Actor {
  serviceManager =>
  val serviceCalls = Map[String, RemoteCall](
    call2tuple(LoginService)
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
  private val sessionManager = new SessionManager

  import scala.reflect.runtime.universe.TypeTag

  private def call2tuple[T <: RemoteCall : TypeTag](rc: T): (String, RemoteCall) = {
    val tag = implicitly[TypeTag[T]]
    tag.tpe.typeSymbol.fullName -> rc
  }

  import ServiceCommand._

  def receive = {
    case WebRequest(sid, clazz, in, p) =>
      import ErrorCode._
      import context.dispatcher
      try {
        val usid = in.fields.get("usid") match {
          case Some(JsString(x)) => x
          case _ => InvalidRequest.raise
        }
        val req = in.fields.getOrElse("request", InvalidRequest.raise)
        val rc = serviceCalls.getOrElse(clazz, NotAcceptable.raise)
        val p2 = Promise[JsValue]
        val task = rc.dispatch(req)
        task.promise = p2
        task.usid = usid
        task match {
          case x: SimpleTask => simpleWorker ! task
          case x: QueryTask => queryWorker ! task
          case x: PersistTask => persistWorker ! task
        }
        p2.future.onComplete {
          case Success(x) =>
            p.success(JsObject("response" -> x))
          case Failure(ex) =>
            p.failure(ex)
        }
      } catch {
        case ex: Throwable => p.failure(ex)
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

        Staffs +=(1L, "000", "admin", Staffs.makePassword("admin"))
      }
      context.system.shutdown()
  }

  class SimpleWorkerActor extends Actor {
    private val requestCountLimit = context.system.settings.config.getInt("weez-mercury.simple-request-count-limit")
    private var workerCount = 0

    def receive = {
      case task: SimpleTask =>
        import context.dispatcher
        if (workerCount < requestCountLimit) {
          Future {
            task.promise.complete(Try(task.call(new Context {
              def clientUsid = task.usid

              def sessionManager = serviceManager.sessionManager
            })))
            self ! Done
          }
        } else {
          task.promise.failure(ErrorCode.Reject)
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
      case task: QueryTask =>
        if (idle.nonEmpty) {
          idle.dequeue() ! task
        } else if (workerCount < maxWorkerCount) {
          val worker = context.actorOf(Props(new WorkerActor))
          worker ! task
          workerCount += 1
        } else if (queue.size + workerCount < requestCountLimit) {
          queue.enqueue(task)
        } else {
          task.promise.failure(ErrorCode.Reject)
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
        case task: QueryTask =>
          task.promise.complete(Try {
            val c = new Context with DBQuery {
              def clientUsid = task.usid

              def sessionManager = serviceManager.sessionManager

              val dbSession = session
            }
            session.withTransaction {
              task.call(c)
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
      case task: PersistTask =>
        task.promise.complete(Try {
          val c = new Context with DBPersist {
            def clientUsid = task.usid

            def sessionManager = serviceManager.sessionManager

            val dbSession = session
          }
          session.withTransaction {
            task.call(c)
          }
        })
    }
  }

  case object Done

}

trait Task {
  var usid: String = _
  var promise: Promise[JsValue] = _
}

case class SimpleTask(call: Context => JsValue) extends Task

case class QueryTask(call: Context with DBQuery => JsValue) extends Task

case class PersistTask(call: Context with DBPersist => JsValue) extends Task

case class WebRequest(sid: String, clazz: String, in: JsObject, promise: Promise[JsValue])

object ServiceCommand extends Enumeration {
  val Setup = Value
}
