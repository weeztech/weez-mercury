package com.weez.mercury.common

object SessionService extends RemoteService {
  def init: NoSessionCall = c => {
    import c._
    val session = app.sessionManager.createSession(peer)
    completeWith("sid" -> session.id)
  }
}
