package com.weez.mercury.common

import spray.json._

case class ErrorCode(code: Int, message: String)

object ErrorCode {
  val InvalidRequest = ErrorCode(800, "the request contains characters cannot be parsed or has invalid format")
  val NotAcceptable = ErrorCode(801, "the request cannot acceptable becuase cannot find RemoteCall")
}

