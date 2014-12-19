package com.weez.mercury.common

import com.weez.mercury.common.Util.RandomIdGenerator

class SessionManager {
  private val sessions = scala.collection.mutable.Map[String, UserSession]()
  private val seed = new RandomIdGenerator(12)

  def findSession(userSessionId: String) = {
    sessions.synchronized {
      sessions.get(userSessionId)
    }
  }

  def createSession(userId: Long) = {
    sessions.synchronized {
      val usid = seed.newId
      val us = new UserSession(usid, userId)
      sessions.put(usid, us)
      us
    }
  }
}

class UserSession(val id: String, val userId: Long) {
  /**
   * NOT A TIME
   */
  val createTimestamp = System.nanoTime()
}
