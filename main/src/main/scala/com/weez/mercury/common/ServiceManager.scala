package com.weez.mercury.common

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
    builder.result()
  }

  case class Handler(f: InternalContext => Unit, sessionState: Boolean, dbQuery: Boolean, dbUpdate: Boolean)

  type InternalContext = Context with SessionState with DBSessionUpdatable

}

class ServiceManager(system: ActorSystem, config: Config) {

  import scala.concurrent._
  import ServiceManager._

  val database: Database = {
    val path = config.getString("database")
    RocksDBBackend.open(Util.resolvePath(path))
  }

  val idAllocSession = database.createSession()

  val dbFactory = new DBSessionFactory(idAllocSession)

  val workerPools = {
    val builder = Seq.newBuilder[WorkerPool]
    val workers = config.getConfig("workers")
    val itor = workers.entrySet().iterator()
    while (itor.hasNext) {
      val e = itor.next()
      val name = e.getKey
      e.getValue match {
        case config: Config =>
          builder += new WorkerPool(name, config)
      }
    }
    builder.result()
  }

  val sessionManager = new SessionManager(config)

  def postRequest(peer: String, sid: String, api: String, req: ModelObject): Future[ModelObject] = {
    import scala.util.control.NonFatal
    import scala.util.Try

    val p = Promise[ModelObject]()
    try {
      val h = remoteCallHandlers.getOrElse(api, ErrorCode.NotAcceptable.raise)
      workerPools.find(wp =>
        wp.permitSessionState == h.sessionState &&
          wp.permitDBQuery == h.dbQuery &&
          wp.permitDBUpdate == h.dbUpdate) match {
        case Some(x) =>
          // get session before worker start to avoid session timeout.
          val sessionState =
            if (x.permitSessionState) {
              new SessionStateImpl(
                sessionManager.getAndLockSession(sid).getOrElse(ErrorCode.InvalidSessionID.raise))
            } else null
          x.post { c =>
            p.complete(Try {
              c.sessionState = sessionState
              c.request = req
              h.f(c)
              if (c.response == null)
                throw new IllegalStateException("no response")
              c.response
            })
            if (sessionState != null)
              sessionManager.returnAndUnlockSession(sessionState.session)
          }
        case None => ErrorCode.NotAcceptable.raise
      }
    } catch {
      case NonFatal(ex) => p.failure(ex)
    }
    p.future
  }

  def close(): Unit = {
    idAllocSession.close()
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
            s"$name-worker-${counter.next()}")
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

  final class ContextImpl extends Context with SessionState with DBSessionUpdatable {

    import akka.event.LoggingAdapter

    var request: ModelObject = _

    var response: ModelObject = _

    var log: LoggingAdapter = _


    def complete(response: ModelObject) = {
      this.response = response
    }

    var sessionState: SessionState = _

    @inline def session = sessionState.session

    @inline def sessionsByPeer(peer: String) = sessionState.sessionsByPeer(peer)

    var dbSessionQuery: DBSessionQueryable = _
    var dbSessionUpdate: DBSessionUpdatable = _

    @inline def get[K: Packer, V: Packer](key: K) = dbSessionQuery.get[K, V](key)

    @inline def exists[K: Packer](key: K) = dbSessionQuery.exists(key)

    @inline def newCursor() = dbSessionQuery.newCursor()

    @inline def getRootCollectionMeta(name: String)(implicit db: DBSessionQueryable) = dbSessionQuery.getRootCollectionMeta(name)

    @inline def newEntityId() = dbSessionUpdate.newEntityId()

    @inline def put[K: Packer, V: Packer](key: K, value: V) = dbSessionUpdate.put(key, value)

    @inline def del[K: Packer](key: K) = dbSessionUpdate.del(key)
  }

  final class SessionStateImpl(val session: Session) extends SessionState {
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
        if (dbSession == null) {
          task.f(taskContext)
        } else {
          dbFactory.withTransaction(dbSession, log) { db =>
            try {
              if (permitDBQuery) taskContext.dbSessionQuery = db
              if (permitDBUpdate) taskContext.dbSessionUpdate = db
              task.f(taskContext)
            } finally {
              taskContext.dbSessionQuery = null
              taskContext.dbSessionUpdate = null
            }
          }
        }
    }
  }

  case class Task(f: ContextImpl => Unit)

}
