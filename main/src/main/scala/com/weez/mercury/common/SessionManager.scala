package com.weez.mercury.common

import java.util.concurrent.TimeUnit

import com.typesafe.config.Config

import scala.collection.mutable
import scala.concurrent.duration._

class SessionManager(config: Config) {
  val sessions = mutable.Map[String, Session]()
  val peers = mutable.Map[String, Seq[Session]]()
  val sidGen = new Util.RandomIdGenerator(12)
  val sessionTimeout = config.getDuration("weez-mercury.http.session-timeout", TimeUnit.NANOSECONDS)
  val checkFreq = 5.seconds
  val devmode = config.getBoolean("weez-mercury.devmode")

  def clean(): Unit = {
    // check session timeout
    val now = System.nanoTime()
    val removes = mutable.ArrayBuffer[String]()
    sessions.synchronized {
      sessions foreach { tp =>
        val session = tp._2
        if (session.inUse == 0 && now - session.activeTimestamp > sessionTimeout) {
          removes.append(session.id)
          peers(session.peer) match {
            case Seq(session) => peers.remove(session.peer)
            case arr => peers.put(session.peer, arr.filterNot(_ eq session))
          }
        }
      }
      removes foreach sessions.remove
    }
  }

  def createPeer(peer: String) = {
    sessions.synchronized {
      // add a fake session to avoid 'clean'
      peers.get(peer) match {
        case Some(x) =>
          x.find(_.id == peer) match {
            case Some(s) =>
              s.activeTimestamp = System.nanoTime()
            case None =>
              val s = new Session(peer, peer)
              peers.put(peer, x :+ s)
          }
          peer
        case None =>
          val peer = sidGen.newId
          val session = new Session(peer, peer)
          if (devmode) {
            session.login(0L, "dev", "dev")
          }
          sessions.put(peer, session)
          peers.put(peer, Seq(session))
          peer
      }
    }
  }

  def createSession(peer: String) = {
    sessions.synchronized {
      val arr = peers.getOrElse(peer, ErrorCode.InvalidPeerID.raise)
      val sid = sidGen.newId
      val session = new Session(sid, peer)
      sessions.put(sid, session)
      peers.put(peer, arr :+ session)
      session
    }
  }

  def getAndLockSession(sid: String) = {
    sessions.synchronized {
      sessions.get(sid).map { s =>
        s.inUse += 1
        s
      }
    }
  }

  def returnAndUnlockSession(session: Session) = {
    sessions.synchronized {
      session.inUse -= 1
      if (session.inUse == 0)
        session.activeTimestamp = System.nanoTime()
    }
  }

  def getSessionsByPeer(peer: String): Seq[Session] = {
    sessions.synchronized {
      peers.get(peer) match {
        case Some(x) => x
        case None => Nil
      }
    }
  }
}

class Session(val id: String, val peer: String) {
  val createTimestamp = System.nanoTime()
  private[common] var inUse = 0
  private[common] var activeTimestamp = createTimestamp

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