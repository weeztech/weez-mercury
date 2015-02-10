package com.weez.mercury.common

import scala.language.dynamics

class ModelObject(private var map: Map[String, Any]) extends Dynamic {
  def selectDynamic[T](field: String): T = {
    map.get(field) match {
      case Some(x) => x.asInstanceOf[T]
      case None => throw new ModelException(s"missing property '$field'")
    }
  }

  def updateDynamic[T](field: String)(value: T): Unit = {
    map += field -> value
  }

  def hasProperty(name: String) = {
    map.contains(name)
  }

  def underlaying = map

  def update(fields: (String, Any)*) = {
    map ++= fields
  }
}

object ModelObject {
  val empty = new ModelObject(Map.empty)

  def apply(fields: (String, Any)*): ModelObject = new ModelObject(fields.toMap)
}

object JsonModel {

  import scala.language.implicitConversions
  import spray.json._

  def from(v: JsValue) = js2model(v)

  def to(v: ModelObject) = model2js(v)

  def parse(v: JsValue) = js2model(v) match {
    case x: ModelObject => x
    case x => throw new ModelException(s"expect ModelObject, buf found $x")
  }

  class JsObjectHelper(val v: JsObject) extends AnyVal {
    def str(field: String) = {
      v.fields.get(field) match {
        case Some(JsString(x)) => x
        case Some(_) => throw new ModelException(s"expect string '$field'")
        case None => throw new ModelException("missing field 'value'")
      }
    }
  }

  implicit def jsObj2helper(v: JsObject): JsObjectHelper = new JsObjectHelper(v)

  private def js2model(jsValue: JsValue): Any = {
    jsValue match {
      case JsNull => null
      case x: JsBoolean => x.value
      case x: JsString => x.value
      case x: JsObject => jsObj2model(x)
      case x: JsArray => x.elements.map(js2model)
      case _ => throw new ModelException(s"unexpected type: $jsValue")
    }
  }

  val ISO8601DateTimeFormatter = org.joda.time.format.ISODateTimeFormat.dateTime()

  private def jsObj2model(v: JsObject): Any = {
    v.fields.get("$tpe") match {
      case Some(JsString(x)) =>
        x match {
          case "Int" => v.str("value").toInt
          case "Long" => v.str("value").toLong
          case "DateTime" => ISO8601DateTimeFormatter.parseDateTime(v.str("value"))
        }
      case _ =>
        new ModelObject(v.fields.map { tp => tp._1 -> js2model(tp._2)})
    }
  }

  private def model2js(v: Any): JsValue = {
    v match {
      case null => JsNull
      case x: Int => num("Int", x.toString)
      case x: Long => num("Long", x.toString)
      case x: Double => num("Double", x.toString)
      case x: org.joda.time.DateTime => num("DateTime", ISO8601DateTimeFormatter.print(x))
      case x: Boolean => JsBoolean(x)
      case x: String => JsString(x)
      case x: Array[_] => array(x)
      case x: Iterable[_] => array(x)
      case x: ModelObject => model2jsObj(x)
    }
  }

  private def model2jsObj(v: ModelObject): JsObject = {
    JsObject(v.underlaying.map { tp =>
      tp._1 -> model2js(tp._2)
    })
  }

  private def num(tpe: String, value: String) =
    JsObject("$tpe" -> JsString(tpe), "value" -> JsString(value))


  private def array(v: Iterable[_]): JsArray = {
    val builder = Vector.newBuilder[JsValue]
    v foreach {
      builder += model2js(_)
    }
    JsArray(builder.result())
  }
}

class ModelException(msg: String) extends Exception(msg)
