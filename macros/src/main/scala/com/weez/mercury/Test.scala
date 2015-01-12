package com.weez.mercury


import spray.json._

import scala.reflect.runtime.{universe => ru, ReflectionUtils}

object Test {
  def main(args: Array[String]): Unit = {
    makePackers()
  }

  def makePackers() = {
    import Template._

    System.out.println {
      rep(22, 2) { n =>
        s"""
         |implicit def tuple[${rt(n)}](tp: (${rt(n)}))(implicit ${r(n, i => s"p$i: Packer[${t(i)}]")}): Packer[(${rt(n)})] =
         |  new ProductPacker[(${rt(n)})](${rv(n, "p")})(a => (${r(n, i => s"a($i).asInstanceOf[${t(i)}]")}))
        """.stripMargin
      }
    }

    System.out.println {
      rep(22) { n =>
        s"""
         |def apply[Z <: Product, ${rt(n)}](f: (${rt(n)}) => Z)(implicit ${r(n, i => s"p$i: Packer[${t(i)}]")}): Packer[Z] =
         |  new ProductPacker[Z](${rv(n, "p")})(a => f(${r(n, i => s"a($i).asInstanceOf[${t(i)}]")}))
        """.stripMargin
      }
    }
  }

}

