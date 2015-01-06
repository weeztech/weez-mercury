package com.weez.mercury.common

import scala.language.dynamics
import scala.language.implicitConversions
import spray.json._
import com.weez.mercury.DB._

import scala.slick.jdbc.{PositionedResult, GetResult}

trait RemoteService {
  type SimpleCall = Context => Unit
  type QueryCall = Context with DBQuery => Unit
  type PersistCall = Context with DBPersist => Unit

  def completeWith(fields: (String, Any)*)(implicit c: Context) = {
    if (c.response == null)
      c.complete(ModelObject(fields: _*))
    else
      c.response.update(fields: _*)
  }

  def failWith(message: String) = {
    throw new ProcessException(ErrorCode.Fail, message)
  }
}

trait Context extends Implicits {
  implicit val context = this

  val session: Session

  def request: ModelObject

  def response: ModelObject

  def complete(response: ModelObject): Unit

  def sessionsByPeer(peer: String = session.peer): Seq[Session]
}

trait DBQuery {
  self: Context =>
  implicit val dbSession: com.weez.mercury.DB.Session
}

trait DBPersist extends DBQuery {
  self: Context =>
}

class ModelObject(private var map: Map[String, Any]) extends Dynamic {
  def selectDynamic[T](field: String): T = {
    map.get(field) match {
      case Some(x) =>
        try {
          x.asInstanceOf[T]
        } catch {
          case ex: ClassCastException => throw new ModelException(s"property '$field' has unexpected type")
        }
      case None => throw new ModelException(s"missing property '$field'")
    }
  }

  def updateDynamic(field: String, value: Any): Unit = {
    map += field -> value
  }

  def hasProperty(name: String) = {
    map.contains(name)
  }

  def update(fields: (String, Any)*) = {
    map ++= fields
  }
}

object ModelObject {
  def apply(fields: (String, Any)*): ModelObject = new ModelObject(fields.toMap)

  def parse[A <: JsValue, B](jsValue: A)(implicit c: Converter[A, B]): B = c(jsValue)

  def toJson[A, B <: JsValue](value: A)(implicit c: Converter[A, B]): B = c(value)

  type Converter[A, B] = A => B

  implicit def js2model(jsValue: JsValue): Any = {
    jsValue match {
      case JsNull => null
      case x: JsBoolean => jsBool2model(x)
      case x: JsNumber => jsNum2model(x)
      case x: JsString => jsStr2model(x)
      case x: JsObject => jsObj2model(x)
      case x: JsArray => jsArray2model(x)
    }
  }

  implicit def jsObj2model(v: JsObject): ModelObject = {
    new ModelObject(v.fields.map { tp => tp._1 -> js2model(tp._2)})
  }

  implicit def jsNum2model(v: JsNumber): AnyVal = {
    if (v.value.isValidInt) v.value.toInt else v.value.toDouble
  }

  implicit def jsBool2model(v: JsBoolean): Boolean = v.value

  implicit def jsStr2model(v: JsString): String = v.value

  implicit def jsArray2model(v: JsArray): Seq[Any] = v.elements.map(js2model)

  implicit def jsNull2model(v: JsNull.type): Null = null

  implicit def model2js(v: Any): JsValue = {
    v match {
      case null => JsNull
      case x: Byte => model2jsNum(x)
      case x: Short => model2jsNum(x)
      case x: Int => model2jsNum(x)
      case x: Long => model2jsNum(x)
      case x: Float => model2jsNum(x)
      case x: Double => model2jsNum(x)
      case x: BigInt => model2jsNum(x)
      case x: BigDecimal => model2jsNum(x)
      case x: Boolean => model2jsBool(x)
      case x: String => model2jsStr(x)
      case x: Array[_] => model2jsArray(x)
      case x: Iterable[_] => model2jsArray(x)
      case x: ModelObject => model2jsObj(x)
    }
  }

  implicit def model2jsObj(v: ModelObject): JsObject = {
    JsObject(v.map.map { tp =>
      tp._1 -> model2js(tp._2)
    })
  }

  implicit def model2jsNum(v: BigInt): JsNumber = JsNumber(v)

  implicit def model2jsNum(v: BigDecimal): JsNumber = JsNumber(v)

  implicit def model2jsNum(v: Byte): JsNumber = JsNumber(v)

  implicit def model2jsNum(v: Short): JsNumber = JsNumber(v)

  implicit def model2jsNum(v: Int): JsNumber = JsNumber(v)

  implicit def model2jsNum(v: Long): JsNumber = JsNumber(v)

  implicit def model2jsNum(v: Float): JsNumber = JsNumber(v: Double)

  implicit def model2jsNum(v: Double): JsNumber = JsNumber(v)

  implicit def model2jsBool(v: Boolean): JsBoolean = JsBoolean(v)

  implicit def model2jsStr(v: String): JsString = JsString(v)

  implicit def model2jsArray(v: Iterable[_]): JsArray = {
    val builder = Vector.newBuilder[JsValue]
    v foreach {
      builder += model2js(_)
    }
    JsArray(builder.result)
  }
}

class ModelException(msg: String) extends Exception(msg)

trait Implicits {
  implicit val getResult4modelobject = {
    GetResult(r => {
      val builder = Map.newBuilder[String, Any]
      val meta = r.rs.getMetaData
      while (r.hasMoreColumns) {
        builder += meta.getColumnName(r.currentPos) -> r.nextObject()
      }
      new ModelObject(builder.result)
    })
  }

  implicit def getResult4caseclass1[A, T](f: A => T)(implicit a: GetResult[A]): GetResult[T] = new GetResult[T] {
    def apply(rs: PositionedResult) = f(rs.<<)
  }

  implicit def getResult4caseclass2[A, B, T](f: (A, B) => T)(implicit a: GetResult[A], b: GetResult[B]): GetResult[T] = new GetResult[T] {
    def apply(rs: PositionedResult) = f(rs.<<, rs.<<)
  }
}
