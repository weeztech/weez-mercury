package com.weez.mercury.common

import scala.language.experimental.macros
import scala.reflect.macros.whitebox.Context
import scala.annotation.StaticAnnotation
import scala.util.matching.Regex

class dbtype extends StaticAnnotation {
  def macroTransform(annottees: Any*): Any = macro DBTypeMacro.impl
}

class DBTypeMacro(val c: Context) extends MacroHelper {

  import c.universe._

  def impl(annottees: c.Expr[Any]*): c.Expr[Any] = {
    withFirst(annottees) {
      val tree = annottees.head.tree
      withClass(tree, Some(Flag.CASE)) { (name, paramss, parents, body, mods, _, _) =>
        val caseClassParents = ensureInherit(parents, tq"_root_.com.weez.mercury.common.Entity")
        val caseClassParams = ensureParam(paramss, q"${Modifiers(Flag.CASEACCESSOR | Flag.PARAMACCESSOR)} val id: Long")
        //val map = getAnnotation(classOf[dbtype].getName, Nil)
        val dbObjectType = tq"_root_.com.weez.mercury.common.DBObjectType[$name]"
        val companionType =
          if (caseClassParams.length == 1) {
            val functionType = makeFunctionTypeWithParamList(caseClassParams, Ident(name))
            dbObjectType :: functionType :: Nil
          } else
            dbObjectType :: Nil
        val packer =
          if (caseClassParams.length == 1)
            q"this"
          else
            flattenFunction(q"apply", caseClassParams)
        val dbname = new Regex("[A-Z]").replaceAllIn(name.toString(), { m =>
          if (m.start == 0)
            m.matched.toLowerCase
          else
            "-" + m.matched.toLowerCase
        })
        val tree = q"""
          $mods class $name(...$caseClassParams) extends ..$caseClassParents { ..$body }

          object ${name.toTermName} extends ..$companionType {
            def nameInDB = ${Literal(Constant(dbname))}
            implicit val packer = _root_.com.weez.mercury.common.Packer($packer)
          }
         """
        //println(show(tree))
        tree
      }
    }
  }
}

