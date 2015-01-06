package com.weez.mercury

import scala.slick.driver._
import scala.slick.jdbc.SQLInterpolation

object DB extends MySQLDriver.SimpleQL {
  implicit def interpolation(s: StringContext) = new SQLInterpolation(s)
}
