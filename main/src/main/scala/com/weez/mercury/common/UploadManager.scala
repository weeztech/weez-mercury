package com.weez.mercury.common

import com.typesafe.config.Config

class UploadManager(app: ServiceManager, config: Config) {

  import java.util.concurrent.TimeUnit
  import akka.actor._

  private val idGen = new Util.RandomIdGenerator(12)
  private val uploads = new TTLMap[String, UploadContextImpl](config.getDuration("start-timeout", TimeUnit.SECONDS))
  private val uploadsClean = app.addTTLCleanEvent(_ => {
    uploads.clean() foreach { x =>
      app.system.stop(x.receiver)
      x.dispose()
    }
  })
  val countLimit = config.getInt("count-limit")

  def openReceive(c: ContextImpl, receiver: () => Actor) = {
    uploads.synchronized {
      val id = idGen.newId
      val ref = app.system.actorOf(Props(receiver()), s"upload-receiver-$id")
      val uc = new UploadContextImpl(id, ref, app, c.api)
      uc.request = c.request
      uc.sessionState = c.sessionState match {
        case null => None
        case x =>
          app.sessionManager.getAndLockSession(x.session.id)
          Some(x)
      }
      uploads.values.put(id, uc)
      uc.id
    }
  }

  def startReceive(id: String) = {
    uploads.synchronized {
      uploads.values.remove(id) match {
        case Some(x) => x
        case None => ErrorCode.InvalidUploadID.raise
      }
    }
  }

  def close() = uploadsClean.close()
}

import akka.actor.ActorRef

class UploadContextImpl(val id: String, val receiver: ActorRef, app: ServiceManager, val api: String) extends TTLBased[String] with UploadContext {

  import scala.concurrent.Promise

  var peer: String = _
  var request: ModelObject = _
  var sessionState: Option[SessionState] = None
  val promise = Promise[Response]()
  var disposed = false

  def finish(response: ModelObject) = {
    promise.success(ModelResponse(response))
    dispose()
  }

  def finishWith[C](f: C => Unit)(implicit evidence: ContextType[C]) = {
    if (evidence.withSessionState)
      require(sessionState.nonEmpty, "session state not available")
    app.remoteCallManager.internalCallback(
      promise,
      evidence.withSessionState,
      evidence.withDBQueryable,
      evidence.withDBUpdatable) { c =>
      c.api = api
      c.peer = peer
      sessionState foreach {
        c.sessionState = _
      }
      c.request = request
      f(c.asInstanceOf[C])
      if (c.response == null)
        throw new IllegalStateException("no response")
      c.response
    }
    dispose()
  }

  def fail(ex: Throwable) = {
    promise.failure(ex)
    dispose()
  }

  def dispose() = {
    disposed = true
    peer = null
    request = null
    sessionState match {
      case Some(x) =>
        app.sessionManager.returnAndUnlockSession(x.session)
        sessionState = None
      case None =>
    }
  }
}




