package com.weez.mercury

object Template {
  def v(i: Int) = {
    ('a' + i).asInstanceOf[Char].toString()
  }

  def t(i: Int) = {
    ('A' + i).asInstanceOf[Char].toString()
  }

  def r(n: Int, f: Int => String): String = {
    join {
      for (i <- 0 until n)
      yield f(i)
    }
  }

  def rep(n: Int, from: Int = 1)(f: Int => String): String = {
    val sb = new StringBuilder
    for (i <- from to n)
      sb.append(f(i))
    sb.toString()
  }


  def rt(n: Int) = r(n, t)

  def rv(n: Int, prefix: String) = r(n, i => prefix + i)

  def join(arr: Seq[String]) = {
    arr.tail.foldLeft(arr.head) { (h, t) =>
      h + ", " + t
    }
  }
}
