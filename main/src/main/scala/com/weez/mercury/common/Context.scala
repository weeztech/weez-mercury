package com.weez.mercury.common

import scala.slick.jdbc.JdbcBackend

trait SimpleContext

trait QueryContext extends SimpleContext {
  implicit val dbsession: JdbcBackend#Session
}

trait PersistContext extends QueryContext
