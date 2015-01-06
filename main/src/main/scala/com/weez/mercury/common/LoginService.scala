package com.weez.mercury.common

import java.util.Arrays
import java.security.MessageDigest
import scala.concurrent._

object LoginService extends RemoteService {
  def login: QueryCall = c => {
    import c._
    val username: String = request.username

    //import slick.jdbc.GetResult._
    val q = sql"select id, code, name, password from biz_staffs where code = $username".as[(Long, String, String, Array[Byte])]
    q.firstOption match {
      case Some((userId, code, name, pass)) =>
        val password = encodePassword(request.password)
        if (!Arrays.equals(password, pass))
          failWith("用户名或密码错误")
        session.login(userId, code, name)
        completeWith("username" -> code, "name" -> name)
      case None => failWith("用户名或密码错误")
    }
  }

  def getLogins: SimpleCall = c => {
    import c._
    val builder = Seq.newBuilder[ModelObject]
    val set = scala.collection.mutable.Set[String]()
    sessionsByPeer() foreach { s =>
      s.loginState match {
        case Some(x) if !set.contains(x.username) =>
          set.add(x.username)
          builder += ModelObject(
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
    val username: String = request.username
    sessionsByPeer() collectFirst {
      case s if s.loginState.exists(_.username == username) =>
        s.loginState.get
    } match {
      case Some(x) =>
        session.login(x.userId, x.username, x.name)
        completeWith("username" -> x.username, "name" -> x.name)
      case None => failWith("用户登录无效")
    }
  }

  def getTime: SimpleCall = c => {
    import c._
    completeWith("epoch" -> System.currentTimeMillis())
  }

  private val digest = new ThreadLocal[MessageDigest]() {
    override def initialValue = MessageDigest.getInstance("MD5")
  }

  def encodePassword(password: String) = {
    digest.get().digest(password.getBytes("UTF-8"))
  }
}
