package com.weez.mercury.common

import scala.concurrent.{ExecutionContext, Future}
import ContextBinding._
import DB.driver.simple._

case class LoginRequest(username: String, password: String)

case class LoginResponse(result: Int)

object LoginService extends ServiceCall[QueryContext, LoginRequest, LoginResponse] {
  val jsonRequest = jsonFormat2(LoginRequest)
  val jsonResponse = jsonFormat1(LoginResponse)

  def call(c: Context, req: LoginRequest): LoginResponse = {
    import c._
    import Staffs._
    import java.util.Arrays
    val q = for (s <- Staffs if s.code === req.username) yield s.password
    q.firstOption match {
      case Some(pass) =>
        val password = makePassword(req.password)
        if (Arrays.equals(password, pass))
          LoginResponse(0)
        else
          LoginResponse(1)
      case None =>
        LoginResponse(2)
    }
  }
}

class Staffs(tag: Tag) extends Table[(String, String, Array[Byte])](tag, "biz_staffs") {
  def code = column[String]("code", O.PrimaryKey)

  def name = column[String]("name")

  def password = column[Array[Byte]]("password")

  def * = (code, name, password)
}

object Staffs extends TableQuery(new Staffs(_)) {
  def makePassword(password: String) = {
    import java.security.MessageDigest
    MessageDigest.getInstance("MD5").digest(password.getBytes)
  }
}
