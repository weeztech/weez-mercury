package com.weez.mercury.common

import com.weez.mercury.common.SessionManager

import scala.language.implicitConversions
import scala.language.existentials
import scala.concurrent._
import scala.util._
import spray.json._
import akka.actor._
import DB.driver.simple._

class ServiceManager(system: ActorSystem) {

  import ServiceManager._

  val sessionManager = new SessionManager(system.settings.config)
  val db = Database.forConfig("weez-mercury.database", system.settings.config)
  val serviceActor = system.actorOf(Props(classOf[ServiceManagerActor], this), "service")

  val remoteServices = Seq(
    LoginService
  )

  val remoteCallHandlers = {
    import scala.reflect.runtime.{universe => ru}
    val builder = Map.newBuilder[String, Task]
    val mirror = ru.runtimeMirror(this.getClass.getClassLoader)
    remoteServices foreach { s =>
      val r = mirror.reflect(s)
      r.symbol.typeSignature.members foreach { member =>
        if (member.isPublic && member.isMethod) {
          val method = member.asMethod
          if (method.paramLists.length == 0 && method.returnType <:< ru.typeOf[_ => Unit]) {
            val func = r.reflectMethod(method)
            val tpe = method.returnType.typeArgs(0)
            val htype =
              if (tpe =:= ru.typeOf[Context with DBPersist])
                HandlerType.Persist
              else if (tpe =:= ru.typeOf[Context with DBQuery])
                HandlerType.Query
              else if (tpe =:= ru.typeOf[Context])
                HandlerType.Simple
              else null
            if (htype != null)
              builder += method.fullName -> Task(htype, func().asInstanceOf[ContextImpl => Unit])
          }
        }
      }
    }
    builder.result
  }

  val tables = Seq(
    com.weez.mercury.common.Staffs,
    com.weez.mercury.product.Products,
    com.weez.mercury.product.ProductPrices
  )

  def createSession() = {
    sessionManager.createSession()
  }

  def postRequest(sid: String, api: String, request: JsObject, p: Promise[JsValue]): Unit = {
    try {
      val session = sessionManager.getAndLockSession(sid).getOrElse(ErrorCode.InvalidSID.raise)
      val task = remoteCallHandlers.getOrElse(api, ErrorCode.NotAcceptable.raise)
      val func = (c: ContextImpl) => {
        p.complete(Try {
          c.request = ModelObject.parse(request)
          task.handle(c)
          c.finish()
        })
        sessionManager.returnAndUnlockSession(session)
      }
      serviceActor ! Task(task.tpe, func)
    } catch {
      case ex: Throwable => p.failure(ex)
    }
  }

  def setup() = {
    db.withTransaction {
      implicit session =>
        tables foreach {
          x =>
            try {
              x.ddl.drop
            } catch {
              case _: Throwable =>
            }
            x.ddl.create
        }

        Staffs +=(1L, "000", "admin", Staffs.makePassword("admin"))
    }
    system.shutdown()
  }
}

object ServiceManager {

  object HandlerType extends Enumeration {
    val Simple, Query, Persist = Value
  }

  sealed case class Task(tpe: HandlerType.Value, handle: ContextImpl => Unit)

  class ServiceManagerActor(serviceManager: ServiceManager) extends Actor {
    val simpleWorker = context.system.actorOf(Props(classOf[SimpleWorkerActor]), "simple-worker")
    val queryWorker = context.system.actorOf(Props(classOf[QueryWorkerActor], serviceManager.db), "query-worker")
    val persistWorker = context.system.actorOf(Props(classOf[PersistWorkerActor], serviceManager.db), "persist-worker")

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
      case Task(_, func) =>
        import context.dispatcher
        if (workerCount < requestCountLimit) {
          Future {
            val c = new ContextImpl(null, null)
            func(c)
            self ! Done
          }
        } else {
          p.failure(ErrorCode.Reject)
        }
      case Done =>
        workerCount -= 1
    }
  }

  class QueryWorkerActor(db: DB.driver.backend.Database) extends Actor {
    private val maxWorkerCount = context.system.settings.config.getInt("weez-mercury.query-worker-count-max")
    private val minWorkerCount = context.system.settings.config.getInt("weez-mercury.query-worker-count-min")
    private val requestCountLimit = context.system.settings.config.getInt("weez-mercury.query-request-count-limit")

    private val queue = scala.collection.mutable.Queue[(RegWithQueryContext, UserSession, Promise[JsValue])]()
    private val idle = scala.collection.mutable.Queue[ActorRef]()
    private var workerCount = 0

    def receive = {
      case task@(_: RegWithQueryContext, _: UserSession, p: Promise[JsValue]) =>
        if (idle.nonEmpty) {
          idle.dequeue() ! task
        } else if (workerCount < maxWorkerCount) {
          val worker = context.actorOf(Props(new WorkerActor))
          worker ! task
          workerCount += 1
        } else if (queue.size + workerCount < requestCountLimit) {
          queue.enqueue(task)
        } else {
          p.failure(ErrorCode.Reject)
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
        case (h: RegWithQueryContext, s: UserSession, p: Promise[JsValue]) =>
          p.complete(Try {
            val c = new ContextImpl(s, dbSession)
            dbSession.withTransaction {
              h.handle(c)
            }
            c.finish()
          })
          sender ! Done
      }
    }

  }

  class PersistWorkerActor(db: DB.driver.backend.Database) extends Actor {
    private val dbSession = db.createSession()

    override def postStop(): Unit = {
      dbSession.close()
    }

    def receive = {
      case (h: RegWithPersistContext, s: UserSession, p: Promise[JsValue]) =>
        p.complete(Try {
          val c = new ContextImpl(s, dbSession)
          dbSession.withTransaction {
            h.handle(c)
          }
          c.finish()
        })
    }
  }

  case object Done

  class ContextImpl(val session: UserSession, val dbSession: DB.driver.simple.Session) extends Context with DBPersist {
    var loginState: Option[LoginState] = None

    var request: ModelObject = null

    var response: ModelObject = null

    def login(userId: Long) = {
      loginState = Some(new LoginStateImpl(userId))
    }

    def logout() = {
      loginState = None
    }

    def complete(response: ModelObject) = {
      this.response = response
    }

    def finish() = {
      if (response == null)
        throw new IllegalStateException("no response")
      ModelObject.toJson(response)
    }
  }

  class LoginStateImpl(val userId: Long) extends LoginState {
  }

}


