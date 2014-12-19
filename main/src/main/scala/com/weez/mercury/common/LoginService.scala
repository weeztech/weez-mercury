package com.weez.mercury.common

import scala.concurrent.{ExecutionContext, Future}
import ContextBinding._
import DB.driver.simple._


case class LoginRequest(username: String, password: String)

case class LoginResponse(result: Int, usid: String)

object LoginService extends ServiceCall[Context with DBQuery, LoginRequest, LoginResponse] {
  val jsonRequest = jsonFormat2(LoginRequest)
  val jsonResponse = jsonFormat2(LoginResponse)

  val getUser = Compiled((username: Column[String]) => {
    for (s <- Staffs if s.code === username) yield (s.id, s.password)
  })

  def call(c: Context, req: LoginRequest): LoginResponse = {
    import java.util.Arrays
    import c._
    getUser(req.username).firstOption match {
      case Some((userId, pass)) =>
        val password = Staffs.makePassword(req.password)
        if (Arrays.equals(password, pass)) {
          val usid = c.changeUser(userId)
          LoginResponse(0, usid)
        } else {
          LoginResponse(1, "")
        }
      case None =>
        LoginResponse(2, "")
    }
  }
}

class Staffs(tag: Tag) extends Table[(Long, String, String, Array[Byte])](tag, "biz_staffs") {
  def id = column[Long]("id", O.PrimaryKey)

  def code = column[String]("code")

  def name = column[String]("name")

  def password = column[Array[Byte]]("password")

  def * = (id, code, name, password)
}

object Staffs extends TableQuery(new Staffs(_)) {
  def makePassword(password: String) = {
    import java.security.MessageDigest
    MessageDigest.getInstance("MD5").digest(password.getBytes)
  }
}
