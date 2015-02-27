package com.weez.mercury.common

import com.typesafe.config.Config

class RemoteCallManager(app: ServiceManager, config: Config) {

  import scala.concurrent._
  import akka.actor._

  val remoteCalls: Map[String, RemoteCall] = {
    import scala.reflect.runtime.universe._
    val mirror = runtimeMirror(this.getClass.getClassLoader)
    val builder = Map.newBuilder[String, RemoteCall]
    def collectCalls(classType: Type, instance: Any) = {
      val instanceMirror = mirror.reflect(instance)
      classType.members foreach { member =>
        if (member.isPublic && member.isMethod) {
          val method = member.asMethod
          if (method.paramLists.isEmpty) {
            val tpe = method.returnType.baseType(typeOf[_ => _].typeSymbol)
            if (!(tpe =:= NoType)) {
              val paramType = tpe.typeArgs(0)
              if (typeOf[FullContext] <:< paramType) {
                builder += method.fullName -> RemoteCall(
                  instanceMirror.reflectMethod(method)().asInstanceOf[FullContext => Unit],
                  paramType <:< typeOf[SessionState],
                  paramType <:< typeOf[DBSessionQueryable],
                  paramType <:< typeOf[DBSessionUpdatable])
              }
            }
          }
        }
      }
    }
    app.types(symbolOf[RemoteService].fullName) withFilter {
      !_.isAbstract
    } foreach { symbol =>
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

  type FullContext = RemoteCallContext with SessionState with DBSessionUpdatable

  case class RemoteCall(f: FullContext => Unit, sessionState: Boolean, dbQuery: Boolean, dbUpdate: Boolean)

  private val workerPools = {
    val builder = Seq.newBuilder[WorkerPool]
    val workers = config.getConfig("workers")
    val itor = workers.root().keySet().iterator()
    while (itor.hasNext) {
      val name = itor.next()
      builder += new WorkerPool(name, workers.getConfig(name))
    }
    builder.result()
  }

  private val uploadIdGen = new Util.SecureIdGenerator(12)
  private val uploads = new TTLMap[String, UploadRequest](
    config.getDuration("upload-timeout", java.util.concurrent.TimeUnit.SECONDS))
  private val uploadsClean = app.addTTLCleanEvent(_ => uploads.clean())

  def postRequest(peer: String, api: String, req: ModelObject): Future[InstantResponse] = {
    import scala.util.control.NonFatal
    try {
      val rc = remoteCalls.getOrElse(api, ErrorCode.RemoteCallNotFound.raise)
      val session = if (rc.sessionState) {
        app.sessionManager
          .getAndLockSession(req.sid)
          .getOrElse(ErrorCode.InvalidSessionID.raise)
      } else null
      workerPools.find { wp =>
        wp.permitContext &&
          wp.permitSessionState == rc.sessionState &&
          wp.permitDBQuery == rc.dbQuery &&
          wp.permitDBUpdate == rc.dbUpdate
      } match {
        case Some(wp) =>
          val p = Promise[InstantResponse]()
          wp.post(RemoteCallTask { c =>
            c.api = api
            c.peer = peer
            if (session != null)
              c.setSession(session)
            c.request = req
            try {
              rc.f(c)
            } finally {
              if (session != null)
                app.sessionManager.returnAndUnlockSession(session)
              processResponse(p, c.response)
            }
          })
          p.future
        case None => Future.failed(ErrorCode.WorkerPoolNotFound.exception)
      }
    } catch {
      case NonFatal(ex) => Future.failed(ex)
    }
  }

  def processResponse(promise: Promise[InstantResponse], resp: Response): Unit = {
    resp match {
      case null => promise.failure(throw new IllegalStateException("no response"))
      case x: InstantResponse => promise.success(x)
      case FutureResponse(x) =>
        import scala.util._
        implicit val executor = internalFutureExecutor
        x.onComplete {
          case Success(r) => processResponse(promise, r)
          case Failure(ex) => promise.failure(ex)
        }
    }
  }

  def registerUpload(c: Context, handler: UploadContext => Unit) = {
    uploads.synchronized {
      val id = uploadIdGen.newId
      val sid = c match {
        case x: RemoteCallContextImpl =>
          if (x.sessionState == null) None else Some(x.sessionState.session.id)
        case x: UploadContext =>
          x.sessionState.map(_.session.id)
        case _ => None
      }
      uploads.values.put(id, new UploadRequest(id, c.api, sid, handler))
      id
    }
  }

  def openUpload(id: String) = {
    uploads.synchronized {
      uploads.values.remove(id) match {
        case Some(x) =>
          val uc = new UploadContextImpl(x.id, app, internalFutureExecutor, x.handler)
          uc.setSession(x.sid)
          uc.api = x.api
          uc
        case None => ErrorCode.InvalidUploadID.raise
      }
    }
  }

  def close() = uploadsClean.close()

  def internalFutureCall[T](permitUpdate: Boolean, f: RemoteCallContextImpl => T) = {
    workerPools find { wp =>
      wp.permitDBQuery && wp.permitDBUpdate == permitUpdate
    } match {
      case Some(wp) =>
        val p = Promise[T]()
        wp.post(RemoteCallTask(c => {
          import scala.util.Try
          p.complete(Try(f(c)))
        }))
        p.future
      case None => Future.failed(ErrorCode.WorkerPoolNotFound.exception)
    }
  }

  private val internalFutureExecutor = new ExecutionContext {
    def execute(runnable: Runnable) = {
      workerPools.find { wp =>
        !wp.permitSessionState && !wp.permitDBQuery && !wp.permitDBUpdate
      } match {
        case Some(wp) => wp.post(InternalFutureTask(runnable))
        case None => throw new IllegalStateException()
      }
    }

    def reportFailure(cause: Throwable) = ()
  }

  class WorkerPool(name: String, config: Config) {

    import akka.actor._

    val counter = Iterator from 0
    val maxWorkerCount = config.getInt("worker-count-max")
    val minWorkerCount = config.getInt("worker-count-min")
    val requestCountLimit = config.getInt("request-count-limit")
    private var workerCount = 0
    private val queue = scala.collection.mutable.Queue[AnyRef]()
    private val idle = scala.collection.mutable.Queue[ActorRef]()
    val permitContext = config.getBoolean("context")
    val permitSessionState = config.getBoolean("session-state")
    val permitDBQuery = config.getBoolean("db-query")
    val permitDBUpdate = config.getBoolean("db-update")

    def post(task: AnyRef): Unit = {
      this.synchronized {
        if (idle.nonEmpty) {
          val worker = idle.dequeue()
          worker ! task
        } else if (workerCount < maxWorkerCount) {
          val worker = app.system.actorOf(Props(newWorker()), s"$name-worker-${counter.next()}")
          worker ! task
          workerCount += 1
        } else if (queue.size + workerCount < requestCountLimit) {
          queue.enqueue(task)
        } else {
          ErrorCode.Reject.raise
        }
      }
    }

    def newWorker(): Actor = {
      if (permitDBQuery || permitDBUpdate) {
        new Actor with ActorLogging {
          // reuse db session, so that worker pool is also pool of db sessions.
          val dbSession: DBSession = app.database.createSession()

          override def postStop() = {
            if (dbSession != null) dbSession.close()
          }

          override def receive = {
            case task: RemoteCallTask =>
              val c = new RemoteCallContextImpl(app, internalFutureExecutor)
              val trans = dbSession.newTransaction(log)
              try {
                c.log = log
                val db = app.dbSessionFactory.create(trans, log)
                if (permitDBQuery) c.dbSessionQuery = db
                if (permitDBUpdate) c.dbSessionUpdate = db
                task.f(c)
                trans.commit()
              } finally {
                c.dispose()
                trans.close()
                done(self)
              }
          }
        }
      } else {
        new Actor with ActorLogging {
          override def receive = {
            case task: RemoteCallTask =>
              val c = new RemoteCallContextImpl(app, internalFutureExecutor)
              try {
                c.log = log
                task.f(c)
              } finally {
                c.dispose()
                done(self)
              }
            case InternalFutureTask(x) =>
              import scala.util.control.NonFatal
              try {
                x.run()
              } catch {
                case NonFatal(ex) =>
                  log.error(ex, "internal future exception")
              }
          }
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

  case class RemoteCallTask(f: RemoteCallContextImpl => Unit)

  case class InternalFutureTask(run: Runnable)

  class UploadRequest(val id: String, val api: String, val sid: Option[String], val handler: UploadContext => Unit) extends TTLBased[String]

}
