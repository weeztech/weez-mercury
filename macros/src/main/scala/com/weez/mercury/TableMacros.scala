package com.weez.mercury

import scala.language.experimental.macros
import scala.reflect.macros.whitebox.Context
import scala.reflect.api.Universe
import scala.reflect.api.Position
import scala.collection.mutable

object TableMacros {
  def defineTable(f: => Unit): Any = macro TableMacrosImpl.defineTable

  def text(len: Int): String = ???

  def int(): Int = ???

  def long(): Long = ???

  def decimal(): BigDecimal = ???
}

object TableDefines {

  sealed trait ColumnDef {
    def name: String
  }

  case class TextColumn(name: String, maxLength: Int) extends ColumnDef

  case class IntColumn(name: String) extends ColumnDef

  case class LongColumn(name: String) extends ColumnDef

  case class DecimalColumn(name: String) extends ColumnDef

}

object TableMacrosImpl {
  def defineTable(c: Context)(f: c.Tree): c.Tree = {
    try {
      buildTableDefine(c.universe)(f)
    } catch {
      case ex: TreeException =>
        c.abort(ex.pos.asInstanceOf[c.Position], ex.getMessage)
    }
  }

  def buildTableDefine(universe: Universe)(f: universe.Tree): universe.Tree = {
    import universe._

    val name = TermName("ABC")
    val liftNameStr = Literal(Constant("biz_" + name.toString.toLowerCase()))
    val liftBody = mutable.ListBuffer[Tree]()
    val defBody = mutable.ListBuffer[Tree]()
    val columns = mutable.ListBuffer[Tree]()
    val columnTypes = mutable.ListBuffer[Tree]()

    f match {
      case q"..$stmts" =>
        stmts foreach {
          case q"def $colName = $exp" =>
            val colNameStr = Literal(Constant(colName.toString.toLowerCase()))
            columns.append(Ident(colName))
            exp match {
              case q"com.weez.mercury.TableMacros.text($len)" =>
                len match {
                  case Literal(Constant(x)) =>
                  case _ => throw new TreeException(len.pos, "expect Literal")
                }
                liftBody.append(q"def $colName = column[String]($colNameStr, O.Length($len, true))")
                defBody.append(q"val $colName = TextColumn($colNameStr, $len)")
                columnTypes.append(tq"String")
              case q"com.weez.mercury.TableMacros.int()" =>
                liftBody.append(q"def $colName = column[Int]($colNameStr)")
                defBody.append(q"val $colName = IntColumn($colNameStr)")
                columnTypes.append(tq"Int")
              case q"com.weez.mercury.TableMacros.long()" =>
                liftBody.append(q"def $colName = column[Long]($colNameStr)")
                defBody.append(q"val $colName = LongColumn($colNameStr)")
                columnTypes.append(tq"Long")
              case q"com.weez.mercury.TableMacros.decimal()" =>
                liftBody.append(q"def $colName = column[BigDecimal]($colNameStr)")
                defBody.append(q"val $colName = DecimalColumn($colNameStr)")
                columnTypes.append(tq"BigDecimal")
              case _ => throw new TreeException(exp.pos, "unknown column define: " + showRaw(exp))
            }
          case q"()" =>
          case x => throw new TreeException(x.pos, "expect field define")
        }
      case _ => throw new TreeException(f.pos, "expect BLOCK")
    }
    val tupleName = Ident(TermName("Tuple" + columns.length))
    liftBody.append(q"def * = $tupleName(..$columns)")
    val tupleTypeName = Ident(TypeName("Tuple" + columns.length))
    defBody.append(q"class TableRows(tag: Tag) extends Table[$tupleTypeName[..$columnTypes]](tag, $liftNameStr) { ..$liftBody }")
    defBody.append(q"private val _query = TableQuery[TableRows]")
    defBody.append(q"def apply() = _query")
    val tree = mutable.ListBuffer[Tree]()
    tree.append(q"import scala.slick.driver.MySQLDriver.simple._")
    tree.append(q"import com.weez.mercury.TableDefines._")
    tree.append(q"class TableDef { ..$defBody }")
    tree.append(q"new TableDef")
    q"..$tree"
  }
}

class TreeException(val pos: Position, msg: String) extends Exception(msg)