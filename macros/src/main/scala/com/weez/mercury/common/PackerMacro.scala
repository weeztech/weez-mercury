package com.weez.mercury.common

import scala.annotation.StaticAnnotation
import scala.language.experimental.macros
import scala.reflect.macros.whitebox.Context

class tuplepackers extends StaticAnnotation {
  def macroTransform(annottees: Any*): Any = macro PackerMacro.tupleImpl
}

class PackerMacro(val c: Context) extends MacroHelper {

  import c.universe._

  def tupleImpl(annottees: c.Expr[Any]*): c.Expr[Any] = {
    val tree =
      annottees.head.tree match {
        case x: ClassDef =>
          ClassDef(x.mods, x.name, x.tparams, Template(x.impl.parents, x.impl.self, x.impl.body ++ makeTupleFunctions()))
        case x: ModuleDef =>
          ModuleDef(x.mods, x.name, Template(x.impl.parents, x.impl.self, x.impl.body ++ makeTupleFunctions()))
        case x =>
          c.error(x.pos, "expect class/trait/object")
          annottees.head.tree
      }
    c.Expr[Any](tree)
  }

  def makeTupleFunctions() = {
    for (i <- 1 to 22) yield {
      val name = TermName(s"tuple$i")
      val typeArgs = makeTypeArgs(i)
      val params = makeTupleParams(typeArgs)
      val tupleType = AppliedTypeTree(Ident(TypeName(s"Tuple$i")), typeArgs)
      var pi = -1
      val packBody = params map { p =>
        pi += 1
        q"""
            ${p.name}.pack(value.${TermName("_" + pi)}, buf, end)
            end += ${p.name}.packLength(value.${TermName("_" + pi)})
         """
      }
      q"""
        implicit def $name[..$typeArgs](..$params): Packer[$tupleType] =
        new Packer[$tupleType] {
          def pack(value: $tupleType, buf: Array[Byte], offset: Int) = {
            buf(offset) = Packer.TYPE_TUPLE
            var end = offset + 1
            ..$packBody
            buf(end) = Packer.TYPE_END
          }
        }
       """
    }
  }

  def makeTypeArgs(n: Int) = {
    var typeArgs: List[Tree] = Nil
    for (i <- 0 until n) {
      val name = ('A' + (n - i - 1)).asInstanceOf[Char]
      typeArgs = Ident(TypeName(name.toString)) :: typeArgs
    }
    typeArgs
  }

  def makeTupleParams(typeArgs: List[Tree]) = {
    var params: List[ValDef] = Nil
    val n = typeArgs.length
    for (i <- 0 until n) {
      val j = n - i - 1
      val p = ValDef(Modifiers(Flag.PARAM | Flag.IMPLICIT), TermName("p" + j), tq"Packer[${typeArgs(j)}]", EmptyTree)
      params = p :: params
    }
    params
  }
}
