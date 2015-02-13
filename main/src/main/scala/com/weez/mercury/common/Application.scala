package com.weez.mercury.common

trait GlobalSettings {

  import com.typesafe.config.Config
  import scala.reflect.runtime.universe._

  def config: Config

  def devmode: Boolean

  def types: Map[String, Seq[Symbol]]
}

trait Application extends GlobalSettings {
  import akka.event.LoggingAdapter

  def system: akka.actor.ActorSystem

  def remoteCallManager: RemoteCallManager

  def sessionManager: SessionManager

  def addTTLCleanEvent(f: LoggingAdapter => Unit): AutoCloseable

  def close(): Unit
}
