package com.weez.mercury.common

import scala.language.experimental.macros
import scala.reflect.macros.whitebox

/*
class dbtype() extends StaticAnnotation {
  def macroTransform(annottees: Any*): Any = macro DBTypeMacro.impl
}*/

class DBTypeMacro(val c: whitebox.Context) extends MacroHelper {

  import c.universe._

  def impl(annottees: c.Tree*): c.Tree = {
    def ensureEntityAsSuperType(parents: List[Tree]) = {
      val entityType = tq"_root_.com.weez.mercury.common.Entity"
      findInherit(parents, entityType) match {
        case Some(_) => parents
        case None => parents :+ entityType
      }
    }

    def ensureIdParam(paramss: List[List[Tree]]) = {
      findParam(paramss, "id", Some(tq"Long")) match {
        case Some(_) => paramss
        case None =>
          val mods = Modifiers(Flag.CASEACCESSOR | Flag.PARAMACCESSOR)
          (q"$mods val id: Long" :: paramss.head) :: paramss.tail
      }
    }

    def addExtraApply(paramss: List[List[Tree]], name: TypeName) = {
      val argss =
        paramss map { params =>
          params map {
            case q"$mod val $name: $tpe = $rhs" =>
              ValDef(Modifiers(Flag.PARAM), name, tpe, rhs)
          }
        }
      var applyArgss =
        argss map { params =>
          params map {
            case ValDef(_, name, _, _) => Ident(name): Tree
          }
        }
      applyArgss = (Literal(Constant(0L)) :: applyArgss.head) :: applyArgss.tail
      q"def apply(...$argss): ${Ident(name)} = apply(...$applyArgss)"
    }

    withException {
      annottees.head match {
        case q"$mods class $name[..$tparams](...$paramss) extends ..$parents { ..$body }" if mods.hasFlag(Flag.CASE) =>
          val caseClassParents = ensureEntityAsSuperType(parents)
          val caseClassParams = ensureIdParam(paramss)
          val dbObjectType = tq"_root_.com.weez.mercury.common.DBObjectType[$name]"
          val companionType =
            if (caseClassParams.length == 1) {
              val functionType = makeFunctionTypeWithParamList(caseClassParams, Ident(name))
              functionType :: dbObjectType :: Nil
            } else
              dbObjectType :: Nil
          val packer =
            if (caseClassParams.length == 1)
              q"this"
            else
              flattenFunction(q"apply", caseClassParams)
          var companionBody =
            q"def nameInDB = ${Literal(Constant(camelCase2sepStyle(name.toString)))}" ::
              q"implicit val packer = _root_.com.weez.mercury.common.Packer($packer)" :: Nil
          if (paramss.head.length < caseClassParams.head.length)
            companionBody = addExtraApply(paramss, name) :: companionBody
          val tree = q"""
          $mods class $name(...$caseClassParams) extends ..$caseClassParents { ..$body }
          object ${name.toTermName} extends ..$companionType { ..$companionBody }
         """
          //println(show(tree))
          tree
        case _ => throw new PositionedException(annottees.head.pos, "expect case class")
      }
    }
  }
}

