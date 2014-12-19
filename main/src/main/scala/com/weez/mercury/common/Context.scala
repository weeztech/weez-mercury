package com.weez.mercury.common

import scala.language.higherKinds
import scala.slick.lifted.CompiledFunction
import DB.driver.simple._

trait Context {
  def sessionManager: SessionManager

  def clientUsid: String

  private var _userSession: Option[UserSession] = sessionManager.findSession(clientUsid)

  def userSession = _userSession

  def userId = _userSession.map(_.userId).getOrElse(0L)

  def changeUser(userId: Long) = {
    val us = sessionManager.createSession(userId)
    _userSession = Some(us)
    us.id
  }
}

trait DBQuery {
  self: Context =>
  implicit val dbSession: Session
}

trait DBPersist extends DBQuery {
  self: Context =>
}