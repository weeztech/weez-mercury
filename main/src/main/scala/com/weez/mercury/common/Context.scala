package com.weez.mercury.common

import scala.slick.jdbc.JdbcBackend
import akka.actor.ActorRef
import org.apache.commons.pool2.impl._
import org.apache.commons.dbcp2._

trait SimpleContext

trait QueryContext extends SimpleContext {
  implicit val dbsession: JdbcBackend#Session
}

trait PersistContext extends QueryContext

sealed trait ContextBinding[C] {
  def createContext(factory: ContextFactory): C
}

object ContextBinding {

  implicit object SimpleContextBinding extends ContextBinding[SimpleContext] {
    def createContext(factory: ContextFactory) = factory.createSimpleContext()
  }

  implicit object QueryContextBinding extends ContextBinding[QueryContext] {
    def createContext(factory: ContextFactory) = factory.createQueryContext()
  }

  implicit object PersistContextBinding extends ContextBinding[PersistContext] {
    def createContext(factory: ContextFactory) = factory.createPersistContext()
  }

}

trait ContextFactory {
  def createSimpleContext(): SimpleContext

  def createQueryContext(): QueryContext

  def createPersistContext(): PersistContext
}