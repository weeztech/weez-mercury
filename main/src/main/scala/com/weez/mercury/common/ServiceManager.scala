package com.weez.mercury.common

import com.typesafe.config.Config

import scala.language.implicitConversions
import scala.language.existentials
import scala.concurrent._
import scala.util._
import spray.json._
import akka.actor._

object ServiceManager {
  type RemoteContext = Context

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
                if (paramType =:= ru.typeOf[Context with DBSessionQueryable])
                  HandlerType.Persist
                else if (paramType =:= ru.typeOf[Context with DBSessionUpdatable])
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

  object HandlerType extends Enumeration {
    val Simple, Query, Persist = Value
  }

}

class ServiceManager(system: ActorSystem) {

  import ServiceManager._

  val taskHandlers = Map[HandlerType.Value, TaskHandler](
    HandlerType.Simple -> new TaskHandler("simple"),
    HandlerType.Query -> new TaskHandler("query"),
    HandlerType.Persist -> new TaskHandler("persist")
  )

  val sessionManager = new SessionManager(system.settings.config)

  def postRequest(peer: String, api: String, request: JsObject, p: Promise[JsValue])(implicit executor: ExecutionContext): Unit = {
    try {
      val mo: ModelObject = ModelObject.parse(request)
      val req: ModelObject = if (mo.hasProperty("request")) mo.request else null
      val session = sessionManager.getAndLockSession(mo.sid).getOrElse(ErrorCode.InvalidSessionID.raise)
      p.future.onComplete { _ =>
        sessionManager.returnAndUnlockSession(session)
      }
      val (tpe, handle) = remoteCallHandlers.getOrElse(api, ErrorCode.NotAcceptable.raise)
      taskHandlers(tpe) { c =>
        p.complete(Try {
          c.session = session
          c.request = req
          if (c.dbSession == null)
            handle(c)
          else
            c.dbSession.withTransaction {
              handle(c)
            }
          if (c.response == null)
            throw new IllegalStateException("no response")
          ModelObject.toJson(c.response)
        })
      }
    } catch {
      case ex: Throwable => p.failure(ex)
    }
  }

  class TaskHandler(name: String) {
    val counter = Iterator from 0
    val config = system.settings.config.getConfig(s"weez-mercury.workers.$name")
    val db = {
      var path = system.settings.config.getString(config.getString("database"))
      if (path.startsWith("~")) {
        if (System.getProperty("os.name").startsWith("Windows")) {
          System.getenv("USERPROFILE")
        } else {
          System.getenv("HOME")
        }
      }
    }
    val maxWorkerCount = config.getInt("worker-count-max")
    val minWorkerCount = config.getInt("worker-count-min")
    val requestCountLimit = config.getInt("request-count-limit")
    private var workerCount = 0
    private val queue = scala.collection.mutable.Queue[ContextImpl => Unit]()
    private val idle = scala.collection.mutable.Queue[ActorRef]()

    def apply(func: ContextImpl => Unit): Unit = {
      this.synchronized {
        if (idle.nonEmpty) {
          val worker = idle.dequeue()
          worker ! Task(func, () => done(worker))
        } else if (workerCount < maxWorkerCount) {
          val dbSession = if (db == null) null else db.createSession()
          val worker = system.actorOf(
            Props(new WorkerActor(new ContextImpl(dbSession))),
            s"$name-worker-${counter.next}")
          worker ! Task(func, () => done(worker))
          workerCount += 1
        } else if (queue.size + workerCount < requestCountLimit) {
          queue.enqueue(func)
        } else {
          ErrorCode.Reject.raise
        }
      }
    }

    def done(worker: ActorRef) = {
      this.synchronized {
        if (queue.nonEmpty) {
          worker ! queue.dequeue()
        } else if (workerCount > minWorkerCount) {
          system.stop(worker)
          workerCount -= 1
        } else {
          idle.enqueue(worker)
        }
      }
    }
  }

  case class Task(func: ContextImpl => Unit, callback: () => Unit)

  class WorkerActor(taskContext: ContextImpl) extends Actor {
    def receive = {
      case Task(func, callback) =>
        try {
          func(taskContext)
        } finally {
          callback()
        }
    }
  }

  class ContextImpl extends Context with DBSessionUpdatable {
    var request: ModelObject = _

    var response: ModelObject = _

    var session: Session = _

    def complete(response: ModelObject) = {
      this.response = response
    }

    def sessionsByPeer(peer: String) = {
      sessionManager.getSessionsByPeer(peer)
    }

    def beginTrans(): Unit

    def endTrans(): Unit

    def get[K, V](key: K): V

    def get[K, V](start: K, end: K): Cursor[V]

    def put[K, V](key: K, value: V): Unit
  }

}
