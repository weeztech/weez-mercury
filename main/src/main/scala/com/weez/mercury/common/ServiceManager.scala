package com.weez.mercury.common

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
    new RocksDBDatabaseFactory(this).open(Util.resolvePath(path))
  }
  val sessionManager = new SessionManager(this, config)
  val remoteCallManager = new RemoteCallManager(this, config)
  val uploadManager = new UploadManager(this, config.getConfig("upload"))

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
    uploadManager.close()
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

final class ContextImpl(val app: ServiceManager) extends Context with SessionState with DBSessionUpdatable {

  import akka.event.LoggingAdapter
  import akka.actor.Actor

  var api: String = _

  var request: ModelObject = _

  var response: ModelObject = _

  var log: LoggingAdapter = _

  var peer: String = _

  @inline def acceptUpload(receiver: => Actor) = app.uploadManager.openReceive(this, () => receiver)

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

  def setSession(_session: Session) = {
    sessionState = new SessionState {
      def session = _session

      @inline final def sessionsByPeer(peer: String) = app.sessionManager.getSessionsByPeer(peer)
    }
  }

  def dispose(): Unit = {
    request = null
    response = null
    peer = null
    log = null
    sessionState = null
    dbSessionQuery = null
    dbSessionUpdate = null
  }
}
