package com.weez.mercury.common

import scala.language.dynamics
import scala.collection.mutable
import spray.json._
import DB.driver.simple._

trait RemoteService {
  type SimpleCall = Context => Unit
  type QueryCall = Context with DBQuery => Unit
  type PersistCall = Context with DBPersist => Unit
}

trait Context extends RemoteDirectives {
  implicit val context = this

  val session: UserSession

  def loginState: Option[LoginState]

  def request: ModelObject

  def response: ModelObject

  def login(userId: Long): Unit

  def logout(): Unit

  def complete(response: ModelObject): Unit

}

trait DBQuery {
  self: Context =>
  implicit val dbSession: Session
}

trait DBPersist extends DBQuery {
  self: Context =>
}

trait LoginState {
  def userId: String
}

class ModelObject(val map: mutable.Map[String, Any]) extends Dynamic {
  def selectDynamic[T](field: String): T = {
    map.get(field) match {
      case Some(x) =>
        if (!x.isInstanceOf[T]) throw new ModelException(s"property '$field' has unexpected type")
        x.asInstanceOf[T]
      case None => throw new ModelException(s"missing property '$field'")
    }
  }

  def updateDynamic(field: String, value: Any): Unit = {
    map.put(field, value)
  }
}

object ModelObject {
  def parse[A <: JsValue, B](jsValue: A)(implicit c: Converter[A, B]): B = c(jsValue)

  def toJson[A, B <: JsValue](value: A)(implicit c: Converter[A, B]): B = c(value)

  type Converter[A, B] = A => B

  implicit def js2model(jsValue: JsValue): Any = {
    jsValue match {
      case JsNull => null
      case x: JsBoolean => x
      case x: JsNumber => x
      case x: JsString => x
      case x: JsObject => x
      case x: JsArray => x
    }
  }

  implicit def jsObj2model(v: JsObject): ModelObject = {
    new ModelObject(v.fields.map[_, mutable.Map[String, Any]] { tp => tp._1 -> js2model(tp._2)})
  }

  implicit def jsNum2model(v: JsNumber): AnyVal = {
    if (v.value.isValidInt) v.value.toInt else v.value.toDouble
  }

  implicit def jsNum2model(v: JsBoolean): Boolean = v.value

  implicit def jsStr2model(v: JsString): String = v.value

  implicit def jsArray2model(v: JsArray): Seq[Any] = v.elements.map[_, Seq[Any]](js2model)

  implicit def jsNull2model(v: JsNull.type): Null = null

  implicit def model2js(v: Any): JsValue = {
    v match {
      case null => JsNull
      case x: Int => x
      case x: Double => x
      case x: String => x
      case x: Array[_] => x
      case x: Iterable[_] => x
      case x: ModelObject => x
    }
  }

  implicit def model2jsObj(v: ModelObject): JsObject = {
    JsObject(v.map.map[_, Map[String, JsValue]] { tp =>
      tp._1 -> model2js(v)
    })
  }

  implicit def model2jsNum(v: Int): JsNumber = JsNumber(v)

  implicit def model2jsNum(v: Double): JsNumber = JsNumber(v)

  implicit def model2jsStr(v: String): JsString = JsString(v)

  implicit def model2jsArray(v: Array[_]): JsArray = JsArray(v.map[_, Vector[JsValue]](model2js))

  implicit def model2jsArray(v: Iterable[_]): JsArray = JsArray(v.map[_, Vector[JsValue]](model2js))
}

class ModelException(msg: String) extends Exception(msg)

trait RemoteDirectives {

}