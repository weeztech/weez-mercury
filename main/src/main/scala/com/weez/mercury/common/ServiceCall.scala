package com.weez.mercury.common

import scala.concurrent.Future

trait ServiceCall[O] {
  def call(c: Context): Future[O]
}
