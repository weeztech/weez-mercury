package com.weez.mercury.common

import scala.language.experimental.macros
import scala.reflect.macros.blackbox

// http://stackoverflow.com/questions/27818194/weaktypetag-not-working-in-implicit-macro-with-no-value-argument
// 避免使用对类型参数使用逆变或协变，因为会导致类型推断总是返回Any或Nothing，macro就不能得到正确得到类型了。
// 用强制类型转换解决类型不匹配的问题。
sealed trait TuplePrefixed[A, B] {
  def shouldTuple: Boolean
}

object TuplePrefixed {
  implicit def prefixed[A, B]: TuplePrefixed[A, B] = macro TupleMacro.prefixedImpl[A, B]

  object WithoutTuple extends TuplePrefixed[Nothing, Nothing] {
    def shouldTuple = false
  }

  object WithTuple extends TuplePrefixed[Nothing, Nothing] {
    def shouldTuple = true
  }

}

class TupleMacro(val c: blackbox.Context) extends MacroHelper {

  import scala.annotation.tailrec
  import c.universe._

  def prefixedImpl[A: c.WeakTypeTag, B: c.WeakTypeTag]: c.Tree = {
    withException {
      val fk = weakTypeOf[A]
      val pk = weakTypeOf[B]
      if (fk.typeSymbol.fullName.startsWith("scala.Tuple")) {
        if (pk.typeSymbol.fullName.startsWith("scala.Tuple")) {
          if (!startsWith(fk.typeArgs, pk.typeArgs))
            throw new PositionedException(c.enclosingPosition, s"cannot use $pk as prefix of $fk")
          reify {
            TuplePrefixed.WithoutTuple.asInstanceOf[TuplePrefixed[A, B]]
          }.tree
        } else if (pk <:< fk.typeArgs.head) {
          reify {
            TuplePrefixed.WithTuple.asInstanceOf[TuplePrefixed[A, B]]
          }.tree
        } else
          throw new PositionedException(c.enclosingPosition, s"cannot use $pk as prefix of $fk")
      } else if (pk <:< fk) {
        reify {
          TuplePrefixed.WithoutTuple.asInstanceOf[TuplePrefixed[A, B]]
        }.tree
      } else
        throw new PositionedException(c.enclosingPosition, s"cannot use $pk as prefix of $fk")
    }
  }

  @tailrec
  private def startsWith(a: List[Type], b: List[Type]): Boolean = {
    if (b.isEmpty)
      true
    else if (a.isEmpty)
      false
    else
      b.head <:< a.head && startsWith(a.tail, b.tail)
  }
}
