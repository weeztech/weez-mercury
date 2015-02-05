package com.weez.mercury.common

import scala.language.implicitConversions

case class ErrorCode(code: Int, message: String) {
  def raise = throw exception

  def exception = new ProcessException(this, message)
}

object ErrorCode {
  val InvalidRequest = ErrorCode(800, "the request cannot be parsed or has invalid format")
  val InvalidPeerID = ErrorCode(801, "the request contains an invalid peer id")
  val InvalidSessionID = ErrorCode(802, "the request contains an invalid session id")
  val InvalidUploadID = ErrorCode(803, "the request contains an invalid upload id")
  val RequestTimeout = ErrorCode(820, "the request cannot be parsed or has invalid format")
  val RemoteCallNotFound = ErrorCode(870, "the request is not acceptable becuase cannot find RemoteCall")
  val WorkerPoolNotFound = ErrorCode(871, "the request is not acceptable becuase cannot find RemoteCall")
  val Reject = ErrorCode(880, "the request is not acceptable because too many requests are waiting")
  val Fail = ErrorCode(899, "user operation failed")
}

class ProcessException(val err: ErrorCode, message: String) extends Exception(message)

