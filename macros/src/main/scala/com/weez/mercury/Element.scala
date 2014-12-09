package com.weez.mercury

trait Element {
  def html(): String
}

class HTMLTag(val name: String,
              val attributes: Seq[(String, JSExpression[_])],
              val children: Seq[Element]) extends Element {
  def html() = {
    ""
  }
}

class HTMLText(content: JSExpression[String]) extends Element {
  def html() = ???
}

trait JSExpression[T]

case class ConstantExpression[T](value: T) extends JSExpression[T]

object NullExpression extends JSExpression[Nothing]


class PolymerTag(define: PolymerDefine) extends Element {
  def html() = ???
}

object HTMLTags {
  implicit def str2exp(s: String): JSExpression[String] = ConstantExpression(s)

  private implicit class AttrsHelper(val attrs: Seq[(String, JSExpression[_])]) extends AnyVal {
    def merge(name: String, value: JSExpression[_]) =
      if (attrs.exists(_._1 == name))
        attrs
      else
        attrs :+ (name -> value)
  }

  def div(attrs: (String, JSExpression[_])*)(children: Element*) = {
    new HTMLTag("div", attrs, children)
  }

  def span(attrs: (String, JSExpression[_])*)(children: Element*) = {
    new HTMLTag("span", attrs, children)
  }

  def button(attrs: (String, JSExpression[_])*)(children: Element*) = {
    new HTMLTag("button", attrs, children)
  }

  def input(attrs: (String, JSExpression[_])*)(children: Element*) = {
    new HTMLTag("input", attrs.merge("type", "text"), children)
  }

  def template[T](bind: JSExpression[List[T]])(f: JSExpression[T] => Seq[Element]) = ???
}

trait DataDefine {
  def field[T](name: String): JSExpression[T]
}

trait PolymerDefine extends DataDefine {
  def apply(attrs: (String, JSExpression[_])*)(children: Element*): PolymerTag
}

