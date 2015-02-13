package com.weez.mercury.common

import scala.concurrent._
import com.typesafe.config.Config
import akka.actor._
import akka.event.LoggingAdapter

class ServiceManager(val system: ActorSystem, val config: Config) extends Application {
  val devmode = config.getBoolean("devmode")
  val types = ClassFinder.collectTypes(system.log).map(tp => tp._1 -> tp._2.toSeq).toMap
  val dbtypeCollector = new DBTypeCollector(types).collectDBTypes(system.log)
  val ttlCleaner = system.actorOf(Props(classOf[TTLCleanerActor], config), "ttl-cleaner")
  val database: Database = {
    val path = config.getString("database")
    new RocksDBDatabaseFactory(this).open(FileIO.resolvePathExp(path))
  }
  val tempDir = FileIO.pathOfExp(config.getString("temp-directory"))
  val sessionManager = new SessionManager(this, config)
  val remoteCallManager = new RemoteCallManager(this, config)

  private val idAllocSession = database.createSession()
  val dbSessionFactory = new DBSessionFactory(idAllocSession)

  def addTTLCleanEvent(f: LoggingAdapter => Unit) = {
    val x = TTLCleanerReg(f)
    ttlCleaner ! x
    new AutoCloseable {
      override def close(): Unit = ttlCleaner ! TTLCleanerUnReg(x)
    }
  }

  def start() = {
    dbtypeCollector.clear()
  }

  system.registerOnTermination {
    remoteCallManager.close()
    sessionManager.close()
  }

  override def close() = {
    system.shutdown()
  }
}

case class TTLCleanerReg(f: LoggingAdapter => Unit)

case class TTLCleanerUnReg(x: TTLCleanerReg)

class TTLCleanerActor(config: Config) extends Actor with ActorLogging {

  import scala.collection.mutable
  import scala.concurrent.duration._
  import java.util.concurrent.TimeUnit

  val freq = config.getDuration("ttl-clean-freq", TimeUnit.SECONDS).seconds
  val cleaners = mutable.ListBuffer[TTLCleanerReg]()

  def receive = {
    case x: TTLCleanerReg =>
      cleaners.append(x)
      if (cleaners.nonEmpty)
        context.setReceiveTimeout(freq)
    case TTLCleanerUnReg(x) =>
      val i = cleaners.indexOf(x)
      if (i >= 0) {
        cleaners.remove(i)
        if (cleaners.isEmpty)
          context.setReceiveTimeout(Duration.Undefined)
      }
    case ReceiveTimeout =>
      context.setReceiveTimeout(freq)
  }

}

trait ContextImpl extends Context {
  var api: String = _
  var response: Response = _

  def app: ServiceManager

  def complete(response: Response) = {
    if (this.response != null) throw new IllegalStateException("already responsed")
    this.response = response
  }

  @inline def acceptUpload(receiver: UploadContext => Unit) = app.remoteCallManager.registerUpload(this, receiver)

  def futureQuery[T](f: DBSessionQueryable => T) = app.remoteCallManager.internalFutureCall(permitUpdate = false, f)

  def futureUpdate[T](f: DBSessionUpdatable => T) = app.remoteCallManager.internalFutureCall(permitUpdate = true, f)

  @inline def tempDir = app.tempDir
}

final class RemoteCallContextImpl(val app: ServiceManager, val executor: ExecutionContext) extends ContextImpl with RemoteCallContext with SessionState with DBSessionUpdatable {

  import akka.event.LoggingAdapter

  var promise = Promise[InstantResponse]()
  var callId: Int = 0
  var request: ModelObject = _
  var peer: String = _
  var log: LoggingAdapter = _
  var sessionState: SessionState = _

  var dbSessionQuery: DBSessionQueryable = _
  var dbSessionUpdate: DBSessionUpdatable = _

  def setSession(_session: Session) = {
    sessionState = new SessionState {
      def session = _session

      @inline final def sessionsByPeer(peer: String) = app.sessionManager.getSessionsByPeer(peer)
    }
  }

  @inline def session = sessionState.session

  @inline def sessionsByPeer(peer: String) = sessionState.sessionsByPeer(peer)


  @inline def get[K: Packer, V: Packer](key: K) = dbSessionQuery.get[K, V](key)

  @inline def exists[K: Packer](key: K) = dbSessionQuery.exists(key)

  @inline def newCursor() = dbSessionQuery.newCursor()

  @inline def getMeta(name: String)(implicit db: DBSessionQueryable) = dbSessionQuery.getMeta(name)

  @inline def newEntityId() = dbSessionUpdate.newEntityId()

  @inline def put[K: Packer, V: Packer](key: K, value: V) = dbSessionUpdate.put(key, value)

  @inline def del[K: Packer](key: K) = dbSessionUpdate.del(key)

  def unuse(): Unit = {
    promise = null
    api = null
    request = null
    peer = null
    response = null
    log = null
    dbSessionQuery = null
    dbSessionUpdate = null
    sessionState = null
  }
}

class UploadContextImpl(val id: String,
                        val app: ServiceManager,
                        val executor: ExecutionContext,
                        val receiver: UploadContext => Unit) extends ContextImpl with UploadContext {
  var sessionState: Option[SessionState] = None
  private var disposed = false
  private val promise = Promise[InstantResponse]()

  private def check(): Unit = require(!disposed)

  def upstream = ???

  def queue = ???

  def isComplete = disposed

  def setSession(sid: Option[String]) = {
    sessionState = sid map { x =>
      app.sessionManager.getAndLockSession(x) match {
        case Some(s) =>
          new SessionState {
            def session = s

            @inline final def sessionsByPeer(peer: String) = app.sessionManager.getSessionsByPeer(peer)
          }
        case None => ErrorCode.InvalidSessionID.raise
      }
    }
  }

  def future = promise.future

  def fail(ex: Throwable) = {
    promise.failure(ex)
    dispose()
  }

  def complete(result: UploadResult) = {
    result match {
      case UploadSuccess(x) => promise.success(x)
      case UploadFailure(ex) => promise.failure(ex)
    }
    dispose()
  }

  private def dispose() = {
    disposed = true
    api = null
    response = null
    sessionState match {
      case Some(x) =>
        app.sessionManager.returnAndUnlockSession(x.session)
        sessionState = None
      case None =>
    }
  }
}
