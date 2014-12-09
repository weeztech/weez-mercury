package com.weez.mercury.common

import akka.actor.Actor

class WorkerActor extends Actor {
  def receive = {
  }
}

class DBTransActor extends Actor {
  def receive = {
    case (rc: ServiceCall[_, _, _], req) =>
      val rc2 = rc.asInstanceOf[ServiceCall[AnyRef, Any, Any]]
      rc2.call(new AnyRef with Session {}, req)
  }
}

class DBQueryActor extends Actor {
  def receive = {
    case (rc: ServiceCall[_, _, _], req) =>
      val rc2 = rc.asInstanceOf[ServiceCall[AnyRef, Any, Any]]
      rc2.call(new AnyRef with Session {}, req)
  }
}
