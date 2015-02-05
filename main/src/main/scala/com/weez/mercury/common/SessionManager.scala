package com.weez.mercury.common

import com.typesafe.config.Config

class SessionManager(app: ServiceManager, config: Config) {

  import java.util.concurrent.TimeUnit
  import scala.concurrent.duration._

  private val timeout = config.getDuration("session-timeout", TimeUnit.NANOSECONDS)
  private val peers = new TTLMap[String, Peer](timeout)
  private val sessions = new TTLMap[String, Session](timeout)
  private val sidGen = new Util.RandomIdGenerator(12)
  private val ttlHandle = app.addTTLCleanEvent(_ => clean())

  def clean(): Unit = {
    sessions.synchronized {
      peers.unlockAll(sessions.clean() map (_.peer))
      peers.clean()
    }
  }

  def ensurePeer(peerHint: Option[String] = None) = {
    sessions.synchronized {
      peerHint flatMap peers.values.get match {
        case Some(x) =>
          x.activeTimestamp = System.nanoTime()
          x.id
        case None =>
          val peer = new Peer(peerHint.getOrElse(sidGen.newId))
          peers.values.put(peer.id, peer)
          if (app.devmode) {
            val session = new Session(peer.id, peer.id)
            session.login(0L, "dev", "dev")
            peer.sessions = session :: peer.sessions
          }
          peer.id
      }
    }
  }

  def createSession(peerId: String) = {
    sessions.synchronized {
      val peer = peers.values.getOrElse(peerId, ErrorCode.InvalidPeerID.raise)
      val sid = sidGen.newId
      val session = new Session(sid, peerId)
      sessions.values.put(sid, session)
      peer.sessions = session :: peer.sessions
      session
    }
  }

  @inline final def getAndLockSession(sid: String) = sessions.lock(sid)

  @inline final def returnAndUnlockSession(session: Session) = sessions.unlock(session)

  @inline final def unlockSession(sid: String) = sessions.unlock(sid)

  def getSessionsByPeer(peerId: String): List[Session] = {
    sessions.synchronized {
      peers.values.get(peerId) match {
        case Some(x) => x.sessions
        case None => Nil
      }
    }
  }

  def close() = {
    sessions.synchronized {
      ttlHandle.close()
      sessions.values.clear()
      peers.values.clear()
    }
  }

  class Peer(val id: String) extends TTLBased[String] {
    var sessions: List[Session] = Nil
  }

}

class Session(val id: String, val peer: String) extends TTLBased[String] {

  import scala.collection.mutable

  private var _loginState: Option[LoginState] = None
  private val map = mutable.Map[String, Any]()

  def loginState = _loginState

  def login(userId: Long, username: String, name: String): Unit = _loginState = Some(LoginState(userId, username, name))

  def logout(): Unit = _loginState = None

  def use[T](f: mutable.Map[String, Any] => T): Unit = {
    map.synchronized {
      f(map)
    }
  }
}

case class LoginState(userId: Long, username: String, name: String)

