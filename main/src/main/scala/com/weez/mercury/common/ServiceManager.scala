package com.weez.mercury.common

import akka.event.LoggingAdapter

import scala.language.implicitConversions
import scala.language.existentials
import scala.concurrent._
import scala.util._
import spray.json._
import akka.actor._
import com.typesafe.config.Config

object ServiceManager {
  val remoteServices = {
    import com.weez.mercury.product._
    Seq(
      LoginService,
      DataService
    )
  }

  val remoteCallHandlers = {
    import scala.reflect.runtime.{universe => ru}
    val builder = Map.newBuilder[String, Handler]
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
              val handler: Handler = if (paramType =:= ru.typeOf[Context with DBSessionQueryable])
                UpdateHandler(r.reflectMethod(method)().asInstanceOf[Context with DBSessionUpdatable => Unit])
              else if (paramType =:= ru.typeOf[Context with DBSessionUpdatable])
                QueryHandler(r.reflectMethod(method)().asInstanceOf[Context with DBSessionQueryable => Unit])
              else if (paramType =:= ru.typeOf[Context])
                SimpleHandler(r.reflectMethod(method)().asInstanceOf[Context => Unit])
              else null
              if (handler != null)
                builder += method.fullName -> handler
            }
          }
        }
      }
    }
    builder.result
  }

  sealed trait Handler {
    def apply(c: Context with DBSessionUpdatable): Unit
  }

  case class SimpleHandler(f: Context => Unit) extends Handler {
    def apply(c: Context with DBSessionUpdatable) = f(c)
  }

  case class QueryHandler(f: Context with DBSessionQueryable => Unit) extends Handler {
    def apply(c: Context with DBSessionUpdatable) = f(c)
  }

  case class UpdateHandler(f: Context with DBSessionUpdatable => Unit) extends Handler {
    def apply(c: Context with DBSessionUpdatable) = f(c)
  }

}

class ServiceManager(system: ActorSystem) {

  import ServiceManager._

  val taskHandlers = Map[Class[_], TaskHandler](
    classOf[SimpleHandler] -> new TaskHandler("simple", _ => new SimpleContext),
    classOf[QueryHandler] -> new TaskHandler("query", db => new QueryContext(db)),
    classOf[UpdateHandler] -> new TaskHandler("update", db => new UpdateContext(db))
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
      val handler = remoteCallHandlers.getOrElse(api, ErrorCode.NotAcceptable.raise)
      taskHandlers(handler.getClass) { c =>
        p.complete(Try {
          c.session = session
          c.request = req
          c.withTransaction {
            handler(c)
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

  class TaskHandler(name: String, contextFactory: Database => ContextImpl) {
    val counter = Iterator from 0
    val config = system.settings.config.getConfig(s"weez-mercury.workers.$name")
    val database = {
      if (config.hasPath("database")) {
        val path = Util.resolvePath(config.getString("database"))
        new RocksDB(path)
      } else null
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
          worker ! newTask(worker, func)
        } else if (workerCount < maxWorkerCount) {
          val worker = system.actorOf(
            Props(new WorkerActor(new ContextImpl(databse))),
            s"$name-worker-${counter.next}")
          worker ! newTask(worker, func)
          workerCount += 1
        } else if (queue.size + workerCount < requestCountLimit) {
          queue.enqueue(func)
        } else {
          ErrorCode.Reject.raise
        }
      }
    }

    def newTask(worker: ActorRef, func: ContextImpl => Unit) = (c: ContextImpl) => {
      try {
        func(c)
      } finally {
        done(worker)
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

  class ContextImpl extends Context with DBSessionUpdatable {
    var request: ModelObject = _

    var response: ModelObject = _

    var session: Session = _

    var log: LoggingAdapter = _

    def complete(response: ModelObject) = {
      this.response = response
    }

    def sessionsByPeer(peer: String) = {
      sessionManager.getSessionsByPeer(peer)
    }

    def withTransaction(f: => Unit): Unit = {

    }
  }

  class SimpleContext extends ContextImpl {
    def withTransaction(f: => Unit) = f

    def get[K: Packer, V: Packer](key: K): V = ???

    def get[K: Packer, V: Packer](start: K, end: K): Cursor[V] = ???

    def put[K: Packer, V: Packer](key: K, value: V): Unit = ???
  }

  class QueryContext(db: Database) extends ContextImpl {
    var dbSession: DBSessionQueryable = _

    def withTransaction(f: => Unit) = {
      db.withQuery(log) { session =>
        dbSession = session
        try {
          f
        } finally {
          dbSession = null
        }
      }
    }

    def get[K: Packer, V: Packer](key: K): V = dbSession.get(key)

    def get[K: Packer, V: Packer](start: K, end: K): Cursor[V] = dbSession.get(start, end)

    def put[K: Packer, V: Packer](key: K, value: V): Unit = ???
  }

  class UpdateContext(db: Database) extends ContextImpl {
    var dbSession: DBSessionUpdatable = _

    def withTransaction(f: => Unit) = {
      db.withUpdate(log) { session =>
        dbSession = session
        try {
          f
        } finally {
          dbSession = null
        }
      }
    }

    def get[K: Packer, V: Packer](key: K): V = dbSession.get(key)

    def get[K: Packer, V: Packer](start: K, end: K): Cursor[V] = dbSession.get(start, end)

    def put[K: Packer, V: Packer](key: K, value: V): Unit = dbSession.put(key, value)
  }

  class WorkerActor(taskContext: ContextImpl) extends Actor with ActorLogging {
    taskContext.log = log

    def receive = {
      case f: (ContextImpl => Unit) => f(taskContext)
    }
  }

}
