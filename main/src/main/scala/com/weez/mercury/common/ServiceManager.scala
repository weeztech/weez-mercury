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

final class RemoteCallContextImpl(val app: ServiceManager, val executor: ExecutionContext) extends RemoteCallContext with SessionState with DBSessionUpdatable {

  import akka.event.LoggingAdapter

  var sessionState: SessionState = _
  var dbSessionQuery: DBSessionQueryable = _
  var dbSessionUpdate: DBSessionUpdatable = _
  var disposed = false

  var request: ModelObject = _
  var peer: String = _
  var log: LoggingAdapter = _
  var api: String = _
  var response: Response = _

  private def check() = require(!disposed)

  def setSession(_session: Session) = {
    sessionState = new SessionState {
      def session = _session

      @inline final def sessionsByPeer(peer: String) = app.sessionManager.getSessionsByPeer(peer)
    }
  }

  def complete(r: Response) = {
    check()
    if (response != null) throw new IllegalStateException("already responsed")
    response = r
  }

  def acceptUpload(receiver: UploadContext => Unit) = {
    check()
    app.remoteCallManager.registerUpload(this, receiver)
  }

  def futureQuery[T](f: DBSessionQueryable => T) = {
    check()
    app.remoteCallManager.internalFutureCall(permitUpdate = false, f)
  }

  def futureUpdate[T](f: DBSessionUpdatable => T) = {
    check()
    app.remoteCallManager.internalFutureCall(permitUpdate = true, f)
  }

  def tempDir = {
    check()
    app.tempDir
  }

  def session = {
    check()
    sessionState.session
  }

  def sessionsByPeer(peer: String) = {
    check()
    sessionState.sessionsByPeer(peer)
  }


  def get[K: Packer, V: Packer](key: K) = {
    check()
    dbSessionQuery.get[K, V](key)
  }

  def exists[K: Packer](key: K) = {
    check()
    dbSessionQuery.exists(key)
  }

  def newCursor() = {
    check()
    dbSessionQuery.newCursor()
  }

  def getMeta(name: String)(implicit db: DBSessionQueryable) = {
    check()
    dbSessionQuery.getMeta(name)
  }

  def newEntityId() = {
    check()
    dbSessionUpdate.newEntityId()
  }

  def put[K: Packer, V: Packer](key: K, value: V) = {
    check()
    dbSessionUpdate.put(key, value)
  }

  def del[K: Packer](key: K) = {
    check()
    dbSessionUpdate.del(key)
  }

  def dispose(): Unit = {
    disposed = true
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
                        val receiver: UploadContext => Unit) extends UploadContext {

  import akka.util.ByteString

  private val promise = Promise[InstantResponse]()
  private var disposed = false
  var input: Pipe.Readable[ByteString] = null

  var api: String = _
  var sessionState: Option[SessionState] = None

  private def check() = require(!disposed)

  def content = {
    check()
    input
  }

  def complete(r: Response) = {
    app.remoteCallManager.processResponse(promise, r)
  }

  def tempDir = {
    check()
    app.tempDir
  }

  def acceptUpload(receiver: UploadContext => Unit) = {
    check()
    app.remoteCallManager.registerUpload(this, receiver)
  }

  def futureQuery[T](f: DBSessionQueryable => T) = {
    check()
    app.remoteCallManager.internalFutureCall(permitUpdate = false, f)
  }

  def futureUpdate[T](f: DBSessionUpdatable => T) = {
    check()
    app.remoteCallManager.internalFutureCall(permitUpdate = true, f)
  }

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

  private def dispose() = {
    disposed = true
    api = null
    sessionState match {
      case Some(x) =>
        app.sessionManager.returnAndUnlockSession(x.session)
        sessionState = None
      case None =>
    }
  }
}
