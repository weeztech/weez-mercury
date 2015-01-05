package com.weez.mercury.common

import scala.language.implicitConversions

case class ErrorCode(code: Int, message: String) {
  def raise = throw exception

  def exception = new ProcessException(this)
}

object ErrorCode {
  val InvalidRequest = ErrorCode(800, "the request cannot be parsed or has invalid format")
  val NotAcceptable = ErrorCode(801, "the request is not acceptable becuase cannot find RemoteCall")
  val Reject = ErrorCode(802, "the request is not acceptable because too many requests are waiting")
  val InvalidPeerID = ErrorCode(803, "the request contains an invalid peer id")
  val InvalidSessionID = ErrorCode(804, "the request contains an invalid session id")

  implicit def errorcode2exception(err: ErrorCode) = err.exception
}

class ProcessException(val err: ErrorCode) extends Exception(err.message)

