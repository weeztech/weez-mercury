package com.weez.mercury.common

trait GlobalSettings {

  import com.typesafe.config.Config
  import scala.reflect.runtime.universe._

  def config: Config

  def devmode: Boolean

  def types: Map[String, Seq[Symbol]]
}

trait Application extends GlobalSettings {

  import akka.actor._

  def system: ActorSystem

  def serviceManager: ServiceManager

  def sessionManager: SessionManager
}