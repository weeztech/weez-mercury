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
              if (ru.typeOf[InternalContext] <:< paramType) {
                builder += method.fullName -> Handler(
                  r.reflectMethod(method)().asInstanceOf[InternalContext => Unit],
                  paramType <:< ru.typeOf[SessionState],
                  paramType <:< ru.typeOf[DBSessionQueryable],
                  paramType <:< ru.typeOf[DBSessionUpdatable])
              }
            }
          }
        }
      }
    }
    builder.result
  }

  case class Handler(f: InternalContext => Unit, sessionState: Boolean, dbQuery: Boolean, dbUpdate: Boolean)

  type InternalContext = Context with SessionState with DBSessionUpdatable
}

class ServiceManager(system: ActorSystem) {

  import ServiceManager._

  val database: Database = {
    val path = system.settings.config.getString("weez.database")
    RocksDBBackend.open(Util.resolvePath(path))
  }

  val workerPools = {
    val builder = Seq.newBuilder[WorkerPool]
    val workers = system.settings.config.getConfig("weez.workers")
    val itor = workers.entrySet().iterator()
    while (itor.hasNext) {
      val e = itor.next()
      val name = e.getKey
      e.getValue match {
        case config: Config =>
          builder += new WorkerPool(name, config)
      }
    }
    builder.result
  }

  val sessionManager = new SessionManager(system.settings.config)

  def postRequest(peer: String, api: String, request: JsObject, p: Promise[JsValue])(implicit executor: ExecutionContext): Unit = {
    try {
      val mo: ModelObject = ModelObject.parse(request)
      val req: ModelObject = if (mo.hasProperty("request")) mo.request else null
      val h = remoteCallHandlers.getOrElse(api, ErrorCode.NotAcceptable.raise)
      workerPools.find(p =>
        p.permitSessionState == h.sessionState &&
          p.permitDBQuery == h.dbQuery &&
          p.permitDBUpdate == h.dbUpdate) match {
        case Some(x) =>
          // get session before worker start to avoid session timeout.
          val sessionState =
            if (x.permitSessionState) {
              new SessionStateImpl(
                sessionManager.getAndLockSession(mo.sid).getOrElse(ErrorCode.InvalidSessionID.raise))
            } else null
          x.post { c =>
            p.complete(Try {
              c.sessionState = sessionState
              c.request = req
              h.f(c)
              if (c.response == null)
                throw new IllegalStateException("no response")
              ModelObject.toJson(c.response)
            })
            if (sessionState != null)
              sessionManager.returnAndUnlockSession(sessionState.session)
          }
        case None => ErrorCode.NotAcceptable.raise
      }
    } catch {
      case ex: Throwable => p.failure(ex)
    }
  }

  def close(): Unit = {
    database.close()
  }

  class WorkerPool(name: String, config: Config) {
    val counter = Iterator from 0
    val maxWorkerCount = config.getInt("worker-count-max")
    val minWorkerCount = config.getInt("worker-count-min")
    val requestCountLimit = config.getInt("request-count-limit")
    val permitSessionState = config.getBoolean("session-state")
    val permitDBQuery = config.getBoolean("db-query")
    val permitDBUpdate = config.getBoolean("db-update")
    private var workerCount = 0
    private val queue = scala.collection.mutable.Queue[ContextImpl => Unit]()
    private val idle = scala.collection.mutable.Queue[ActorRef]()

    def post(func: ContextImpl => Unit): Unit = {
      this.synchronized {
        if (idle.nonEmpty) {
          val worker = idle.dequeue()
          worker ! newTask(worker, func)
        } else if (workerCount < maxWorkerCount) {
          val worker = system.actorOf(
            Props(new WorkerActor(permitDBQuery, permitDBUpdate)),
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

    def newTask(worker: ActorRef, func: ContextImpl => Unit) = {
      Task { c =>
        try {
          func(c)
        } finally {
          done(worker)
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

  class ContextImpl extends Context with SessionState with DBSessionUpdatable {
    var request: ModelObject = _

    var response: ModelObject = _

    var log: LoggingAdapter = _


    def complete(response: ModelObject) = {
      this.response = response
    }

    var sessionState: SessionState = _

    @inline def session = sessionState.session

    @inline def sessionsByPeer(peer: String) = sessionState.sessionsByPeer(peer)

    var dbTransQuery: DBTransaction = _
    var dbTransUpdate: DBTransaction = _

    @inline def get[K: Packer, V: Packer](key: K): Option[V] = dbTransQuery.get[K, V](key)

    @inline def newCursor[K: Packer, V: Packer]: DBCursor[K, V] = dbTransQuery.newCursor[K, V]

    @inline def put[K: Packer, V: Packer](key: K, value: V): Unit = dbTransUpdate.put(key, value)

    override def del[K: Packer](key: K): Unit = dbTransUpdate.del(key)

    override def exists[K: Packer](key: K): Boolean = dbTransQuery.exists(key)

    override private[common] def newUInt48: Long = dbTransUpdate.newUInt48

    override private[common] def ensureDefineID(fullName: String): Int =
      if (dbTransQuery != null) dbTransQuery.ensureDefineID(fullName) else dbTransUpdate.ensureDefineID(fullName)
  }

  class SessionStateImpl(val session: Session) extends SessionState {
    @inline def sessionsByPeer(peer: String = session.peer) = sessionManager.getSessionsByPeer(peer)
  }

  class WorkerActor(permitDBQuery: Boolean, permitDBUpdate: Boolean) extends Actor with ActorLogging {
    val taskContext = new ContextImpl
    // reuse db session, so that worker pool is also pool of db sessions.
    val dbSession: DBSession = {
      if (permitDBQuery || permitDBUpdate)
        database.createSession()
      else null
    }

    taskContext.log = log

    override def postStop() = {
      if (dbSession != null) dbSession.close()
    }

    def receive = {
      case task: Task =>
        dbSession.withTransaction(log) { trans =>
          try {
            if (permitDBQuery) taskContext.dbTransQuery = trans
            if (permitDBUpdate) taskContext.dbTransUpdate = trans
            task.f(taskContext)
          } finally {
            taskContext.dbTransQuery = null
            taskContext.dbTransUpdate = null
          }
        }
    }
  }

  case class Task(f: ContextImpl => Unit)

}
