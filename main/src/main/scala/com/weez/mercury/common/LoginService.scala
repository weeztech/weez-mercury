package com.weez.mercury.common

import java.util.Arrays

import scala.concurrent._
import DB.driver.simple._

import scala.slick.ast.ColumnOption.DBType

object LoginService extends RemoteService {
  def login: QueryCall = c => {
    import c._
    val username: String = request.username
    val q = for (s <- Staffs if s.code === username) yield (s.id, s.code, s.name, s.password)
    q.firstOption match {
      case Some((userId, code, name, pass)) =>
        val password = Staffs.makePassword(request.password)
        if (!Arrays.equals(password, pass))
          failWith("用户名或密码错误")
        session.login(userId, code, name)
        completeWith("success" -> 1)
      case None => failWith("用户名或密码错误")
    }
  }

  def getLogins: SimpleCall = c => {
    import c._
    val builder = Seq.newBuilder[ModelObject]
    val set = scala.collection.mutable.Set[Long]()
    sessionsByPeer() foreach { s =>
      s.loginState match {
        case Some(x) if !set.contains(x.userId) =>
          set.add(x.userId)
          builder += ModelObject(
            "userId" -> x.userId,
            "username" -> x.username,
            "name" -> x.name
          )
        case _ =>
      }
    }
    completeWith("logins" -> builder.result.sortBy[String](_.name))
  }

  def quickLogin: SimpleCall = c => {
    import c._
    val userId: String = request.userId
    sessionsByPeer() collectFirst {
      case s if s.loginState.exists(_.userId == userId) =>
        s.loginState.get
    } match {
      case Some(x) =>
        session.login(x.userId, x.username, x.name)
        completeWith("success" -> 1)
      case None => failWith("用户登录无效")
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

  import java.security.MessageDigest
  private val digest = new ThreadLocal[MessageDigest]() {
    override def initialValue = MessageDigest.getInstance("MD5")
  }

  def makePassword(password: String) = {
    digest.get().digest(password.getBytes("UTF-8"))
  }
}
