package com.weez.mercury.common

import com.weez.mercury.DB

import scala.language.implicitConversions
import scala.language.existentials
import scala.concurrent._
import scala.util._
import spray.json._
import akka.actor._
import DB.driver.simple._

object ServiceManager {
  type RemoteContext = Context with DBPersist

  val remoteServices = {
    import com.weez.mercury.product._
    Seq(
      LoginService,
      DataService
    )
  }

  val remoteCallHandlers = {
    import scala.reflect.runtime.{universe => ru}
    val builder = Map.newBuilder[String, (HandlerType.Value, RemoteContext => Unit)]
    val mirror = ru.runtimeMirror(this.getClass.getClassLoader)
    remoteServices foreach { s =>
      val r = mirror.reflect(s)
      r.symbol.typeSignature.members foreach { member =>
        if (member.isPublic && member.isMethod) {
          val method = member.asMethod
          if (method.paramLists.isEmpty) {
            val tpe = method.returnType.baseType(ru.typeOf[Function1[_, _]].typeSymbol)
            if (!(tpe =:= ru.NoType)) {
              val paramType = tpe.typeArgs(0)
              val htype =
                if (paramType =:= ru.typeOf[Context with DBPersist])
                  HandlerType.Persist
                else if (paramType =:= ru.typeOf[Context with DBQuery])
                  HandlerType.Query
                else if (paramType =:= ru.typeOf[Context])
                  HandlerType.Simple
                else null
              if (htype != null) {
                val func = r.reflectMethod(method)().asInstanceOf[RemoteContext => Unit]
                builder += method.fullName -> (htype -> func)
              }
            }
          }
        }
      }
    }
    builder.result
  }

  val tables = {
    import com.weez.mercury._
    Seq(
      common.Staffs,
      product.ProductModels,
      product.Products,
      product.ProductPrices,
      product.Assistants,
      product.Rooms
    )
  }

  object HandlerType extends Enumeration {
    val Simple, Query, Persist = Value
  }

  sealed case class Task(tpe: HandlerType.Value, func: DB.driver.simple.Session => JsValue, p: Promise[JsValue])

  class ServiceManagerActor extends Actor {
    val simpleWorker = context.system.actorOf(Props(classOf[SimpleWorkerActor]), "simple-worker")
    val queryWorker = context.system.actorOf(Props(classOf[QueryWorkerActor]), "query-worker")
    val persistWorker = context.system.actorOf(Props(classOf[PersistWorkerActor]), "persist-worker")

    def receive = {
      case x: Task =>
        x.tpe match {
          case HandlerType.Persist => simpleWorker ! x
          case HandlerType.Query => queryWorker ! x
          case HandlerType.Simple => persistWorker ! x
        }
    }
  }

  class SimpleWorkerActor extends Actor {
    private val requestCountLimit = context.system.settings.config.getInt("weez-mercury.simple-request-count-limit")
    private var workerCount = 0

    def receive = {
      case Task(_, func, p) =>
        import context.dispatcher
        if (workerCount < requestCountLimit) {
          Future {
            p.complete(Try {
              func(null)
            })
            self ! Done
          }
        } else {
          p.failure(ErrorCode.Reject.exception)
        }
      case Done =>
        workerCount -= 1
    }
  }

  class QueryWorkerActor extends Actor {
    val maxWorkerCount = context.system.settings.config.getInt("weez-mercury.query-worker-count-max")
    val minWorkerCount = context.system.settings.config.getInt("weez-mercury.query-worker-count-min")
    val requestCountLimit = context.system.settings.config.getInt("weez-mercury.query-request-count-limit")
    val db = Database.forConfig("weez-mercury.database.readonly", context.system.settings.config)

    val queue = scala.collection.mutable.Queue[Task]()
    val idle = scala.collection.mutable.Queue[ActorRef]()
    var workerCount = 0

    def receive = {
      case task@Task(_, _, p) =>
        if (idle.nonEmpty) {
          idle.dequeue() ! task
        } else if (workerCount < maxWorkerCount) {
          val worker = context.actorOf(Props(new WorkerActor))
          worker ! task
          workerCount += 1
        } else if (queue.size + workerCount < requestCountLimit) {
          queue.enqueue(task)
        } else {
          p.failure(ErrorCode.Reject.exception)
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
      private val dbSession = db.createSession()

      override def postStop(): Unit = {
        dbSession.close()
      }

      def receive = {
        case Task(_, func, p) =>
          p.complete(Try {
            dbSession.withTransaction {
              func(dbSession)
            }
          })
          sender ! Done
      }
    }

  }

  class PersistWorkerActor extends Actor {
    val db = Database.forConfig("weez-mercury.database.writable", context.system.settings.config)
    val dbSession = db.createSession()

    override def postStop(): Unit = {
      dbSession.close()
    }

    def receive = {
      case Task(_, func, p) =>
        p.complete(Try {
          dbSession.withTransaction {
            func(dbSession)
          }
        })
    }
  }

  case object Done

}

class ServiceManager(system: ActorSystem) {

  import ServiceManager._

  val sessionManager = new SessionManager(system.settings.config)
  val serviceActor = system.actorOf(Props(classOf[ServiceManagerActor]), "service")

  def postRequest(peer: String, api: String, request: JsObject, p: Promise[JsValue])(implicit executor: ExecutionContext): Unit = {
    try {
      val mo: ModelObject = ModelObject.parse(request)
      val s = sessionManager.getAndLockSession(mo.sid).getOrElse(ErrorCode.InvalidSessionID.raise)
      val (tpe, handle) = remoteCallHandlers.getOrElse(api, ErrorCode.NotAcceptable.raise)
      val func = (db: DB.driver.simple.Session) => {
        val c = new ContextImpl {
          val session = s
          val dbSession = db
        }
        c.request = if (mo.hasProperty("request")) mo.request else null
        handle(c)
        c.finish()
      }
      serviceActor ! Task(tpe, func, p)
      p.future.onComplete { _ =>
        sessionManager.returnAndUnlockSession(s)
      }
    } catch {
      case ex: Throwable => p.failure(ex)
    }
  }

  trait ContextImpl extends Context with DBPersist {
    var request: ModelObject = null

    var response: ModelObject = null

    def complete(response: ModelObject) = {
      this.response = response
    }

    def sessionsByPeer(peer: String) = {
      sessionManager.getSessionsByPeer(peer)
    }

    def finish() = {
      if (response == null)
        throw new IllegalStateException("no response")
      ModelObject.toJson(response)
    }
  }

}
