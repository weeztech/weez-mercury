package com.weez.mercury.common

import scala.slick.driver._

object DB {
  val driver = MySQLDriver

  import driver.simple._

  val fulltextMatch = SimpleExpression.binary[String, String, Boolean] { (col, search, qb) =>
    qb.sqlBuilder += "match("
    qb.expr(col)
    qb.sqlBuilder += ") against ("
    qb.expr(search)
    qb.sqlBuilder += " in boolean mode)"
  }
}
