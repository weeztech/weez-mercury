package com.weez.mercury

import spray.json._

case class Person(name: Name)

case class Name(firstName: String, lastName: String)

object PersonJsonSupport extends DefaultJsonProtocol {
  implicit val name = jsonFormat2(Name)
  implicit val person = jsonFormat1(Person)
}

