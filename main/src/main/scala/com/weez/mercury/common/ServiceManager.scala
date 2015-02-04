package com.weez.mercury.common

import com.typesafe.config.Config

class ServiceManager(app: Application, config: Config) {
  serviceManager =>

  import scala.concurrent._
  import akka.actor._

  def remoteCalls: Map[String, RemoteCall] = {
    import scala.reflect.runtime.universe._
    val mirror = runtimeMirror(this.getClass.getClassLoader)
    val builder = Map.newBuilder[String, RemoteCall]
    def collectCalls(classType: Type, instance: Any) = {
      val instanceMirror = mirror.reflect(instance)
      classType.members foreach { member =>
        if (member.isPublic && member.isMethod) {
          val method = member.asMethod
          if (method.paramLists.isEmpty) {
            val tpe = method.returnType.baseType(typeOf[Function1[_, _]].typeSymbol)
            if (!(tpe =:= NoType)) {
              val paramType = tpe.typeArgs(0)
              if (typeOf[ContextImpl] <:< paramType) {
                builder += method.fullName -> RemoteCall(
                  instanceMirror.reflectMethod(method)().asInstanceOf[ContextImpl => Unit],
                  paramType <:< typeOf[SessionState],
                  paramType <:< typeOf[DBSessionQueryable],
                  paramType <:< typeOf[DBSessionUpdatable])
              }
            }
          }
        }
      }
    }
    app.types("com.weez.mercury.common.RemoteService") withFilter {
      !_.isAbstract
    } foreach { symbol =>
      var instance: Any = null
      if (symbol.isModule) {
        collectCalls(symbol.typeSignature, mirror.reflectModule(symbol.asModule).instance)
      } else {
        val classSymbol = symbol.asClass
        val classType = classSymbol.toType
        val ctorSymbol = classType.member(termNames.CONSTRUCTOR).asMethod
        if (ctorSymbol.paramLists.flatten.nonEmpty)
          throw new IllegalStateException(s"expect no arguments or abstract: ${classSymbol.fullName}")
        val ctorMirror = mirror.reflectClass(classSymbol).reflectConstructor(ctorSymbol)
        collectCalls(classType, ctorMirror())
      }
    }
    builder.result()
  }

  case class RemoteCall(f: ContextImpl => Unit, sessionState: Boolean, dbQuery: Boolean, dbUpdate: Boolean)

  val database: Database = {
    val path = config.getString("database")
    new RocksDBDatabaseFactory(app).open(Util.resolvePath(path))
  }

  val idAllocSession = database.createSession()

  val dbFactory = new DBSessionFactory(idAllocSession)

  val workerPools = {
    val builder = Seq.newBuilder[WorkerPool]
    val workers = config.getConfig("workers")
    val itor = workers.root().keySet().iterator()
    while (itor.hasNext) {
      val name = itor.next()
      builder += new WorkerPool(name, workers.getConfig(name))
    }
    builder.result()
  }

  def postRequest(peer: String, api: String, req: ModelObject): Future[ModelObject] = {
    import scala.util.Try
    val rc = remoteCalls.getOrElse(api, ErrorCode.RemoteCallNotFound.raise)
    val wp =
      workerPools.find { wp =>
        wp.permitSessionState == rc.sessionState &&
          wp.permitDBQuery == rc.dbQuery &&
          wp.permitDBUpdate == rc.dbUpdate
      }.getOrElse(ErrorCode.WorkerPoolNotFound.raise)
    val sessionState =
      if (wp.permitSessionState) {
        new SessionStateImpl(app.sessionManager
          .getAndLockSession(req.sid)
          .getOrElse(ErrorCode.InvalidSessionID.raise))
      } else null
    val p = Promise[ModelObject]()
    wp.post { c =>
      p.complete(Try {
        c.peer = peer
        c.sessionState = sessionState
        c.request = req
        rc.f(c)
        if (c.response == null)
          throw new IllegalStateException("no response")
        c.response
      })
      if (sessionState != null)
        app.sessionManager.returnAndUnlockSession(sessionState.session)
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
          val worker = app.system.actorOf(
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
          app.system.stop(worker)
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

    var peer: String = _

    def app = serviceManager.app

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
    @inline def sessionsByPeer(peer: String = session.peer) = app.sessionManager.getSessionsByPeer(peer)
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
