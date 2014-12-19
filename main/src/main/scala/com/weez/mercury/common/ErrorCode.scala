package com.weez.mercury.common

import scala.language.implicitConversions

case class ErrorCode(code: Int, message: String) {
  def raise = throw exception

  def exception = new ProcessException(this)
}

object ErrorCode {
  val InvalidRequest = ErrorCode(800, "the request contains characters cannot be parsed or has invalid format")
  val NotAcceptable = ErrorCode(801, "the request is not acceptable becuase cannot find RemoteCall")
  val Reject = ErrorCode(802, "the request is not acceptable because too many requests are waiting")
  val InvalidUSID = ErrorCode(803, "the request contains no usid or an invalid usid")

  implicit def errorcode2exception(err: ErrorCode) = err.exception
}

class ProcessException(val err: ErrorCode) extends Exception(err.message)

