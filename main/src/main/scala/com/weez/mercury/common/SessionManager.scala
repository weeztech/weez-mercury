package com.weez.mercury.common

import java.util.concurrent.TimeUnit

import com.typesafe.config.Config

import scala.collection.mutable
import scala.concurrent.duration._

class SessionManager(config: Config) {
  val sessions = mutable.Map[String, UserSession]()
  val sidGen = new Util.RandomIdGenerator(12)
  val sessionTimeout = config.getDuration("weez-mercury.http.session-timeout", TimeUnit.NANOSECONDS)
  val checkFreq = 5 seconds

  def clean(): Unit = {
    // check session timeout
    val now = System.nanoTime()
    val arr = mutable.ArrayBuffer[String]()
    sessions.synchronized {
      sessions foreach { tp =>
        if (tp._2.inUse == 0 && now - tp._2.activeTimestamp > sessionTimeout)
          arr.append(tp._1)
      }
      arr foreach sessions.remove
    }
  }

  def createSession() = {
    sessions.synchronized {
      val sid = sidGen.newId
      val session = new UserSession(sid)
      sessions.put(sid, session)
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

  def returnAndUnlockSession(session: UserSession) = {
    sessions.synchronized {
      session.inUse -= 1
      if (session.inUse == 0)
        session.activeTimestamp = System.nanoTime()
    }
  }
}


class UserSession(val id: String) {
  val createTimestamp = System.nanoTime()
  private val map = mutable.Map[String, Any]()
  private[SessionManager] var inUse = 0
  private[SessionManager] var activeTimestamp = System.nanoTime()

  def use[T](f: mutable.Map[String, Any] => T) = {
    map.synchronized {
      f(map)
    }
  }
}

class ViewSession(val id: String, val userId: Long) {
  private val map = mutable.Map[String, Any]()

  def use[T](f: mutable.Map[String, Any] => T): Unit = {
    map.synchronized {
      f(map)
    }
  }
}
