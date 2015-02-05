package com.weez.mercury.common

import com.typesafe.config.Config

class RemoteCallManager(app: ServiceManager, config: Config) {

  import scala.concurrent._

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
    app.types(symbolOf[RemoteService].fullName) withFilter {
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
    val session =
      if (wp.permitSessionState) {
        app.sessionManager
          .getAndLockSession(req.sid)
          .getOrElse(ErrorCode.InvalidSessionID.raise)
      } else null
    val p = Promise[ModelObject]()
    wp.post(Task({ c =>
      p.complete(Try {
        c.peer = peer
        if (session != null)
          c.setSession(session)
        c.request = req
        rc.f(c)
        if (c.response == null)
          throw new IllegalStateException("no response")
        c.response
      })
      if (session != null)
        app.sessionManager.returnAndUnlockSession(session)
    }))
    p.future
  }

  def internalCallback(promise: Promise[ModelObject],
                       peer: String,
                       sessionState: Option[SessionState],
                       req: ModelObject,
                       permitDBQuery: Boolean,
                       permitDBUpdate: Boolean)(f: ContextImpl => Unit) = {
    import scala.util.Try
    workerPools.find { wp =>
      wp.permitSessionState == sessionState.isDefined &&
        wp.permitDBQuery == permitDBQuery &&
        wp.permitDBUpdate == permitDBUpdate
    } match {
      case Some(wp) =>
        wp.post(Task({ c =>
          promise.complete(Try {
            c.peer = peer
            sessionState foreach {
              c.sessionState = _
            }
            c.request = req
            f(c)
            if (c.response == null)
              throw new IllegalStateException("no response")
            c.response
          })
          if (sessionState.isDefined)
            app.sessionManager.returnAndUnlockSession(sessionState.get.session)
        }))
      case None => promise.failure(ErrorCode.WorkerPoolNotFound.exception)
    }
  }

  def close() = ()

  class WorkerPool(name: String, config: Config) {

    import akka.actor._

    val counter = Iterator from 0
    val maxWorkerCount = config.getInt("worker-count-max")
    val minWorkerCount = config.getInt("worker-count-min")
    val requestCountLimit = config.getInt("request-count-limit")
    private var workerCount = 0
    private val queue = scala.collection.mutable.Queue[Task]()
    private val idle = scala.collection.mutable.Queue[ActorRef]()
    val permitSessionState = config.getBoolean("session-state")
    val permitDBQuery = config.getBoolean("db-query")
    val permitDBUpdate = config.getBoolean("db-update")

    def post(task: Task): Unit = {
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
            case task: Task =>
              val c = new ContextImpl(app)
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
            case task: Task =>
              val c = new ContextImpl(app)
              try {
                c.log = log
                task.f(c)
              } finally {
                c.dispose()
                done(self)
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

  case class Task(f: ContextImpl => Unit)

}
