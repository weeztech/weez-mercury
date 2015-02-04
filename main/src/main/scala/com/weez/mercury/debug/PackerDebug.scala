package com.weez.mercury.debug

object PackerDebug {

  import com.weez.mercury.common._

  case class ShowOptions(offset: Boolean = false, tpe: Boolean = false, wrap: Boolean = false, hex: Boolean = false)

  def show(buf: Array[Byte], options: ShowOptions): String = show(buf, 0, buf.length, options)

  def show(buf: Array[Byte], start: Int, end: Int, options: ShowOptions): String = {
    new ShowContext(buf, start, end, options).show()
  }

  trait ShowPrinter[T] {
    def tpe: String

    def apply(options: ShowOptions, value: T): String
  }

  object ShowPrinter {
    def apply[T](_tpe: String)(f: (ShowOptions, T) => String): ShowPrinter[T] =
      new ShowPrinter[T] {
        def tpe = _tpe

        def apply(options: ShowOptions, value: T) = f(options, value)
      }

    implicit val stringPrinter = apply("String") { (_, v: String) =>
      val re = "\t|\n|\r|\"".r
      val s =
        re.replaceAllIn(v, m => {
          m.toString() match {
            case "\t" => "\\t"
            case "\n" => "\\n"
            case "\r" => "\\r"
            case "\"" => "\\\""
          }
        })
      "\"" + s + "\""
    }

    implicit val intPrinter = apply("Int") { (o, v: Int) =>
      if (o.hex) {
        val i = f"$v%x"
        "0" * (8 - i.length) + i
      } else
        v.toString
    }

    implicit val longPrinter = apply("Long") { (o, v: Long) =>
      if (o.hex) {
        val i = f"$v%xL"
        "0" * (17 - i.length) + i
      } else
        v.toString
    }

    implicit val booleanPrinter = apply("Boolean") { (_, v: Boolean) =>
      v.toString
    }

    implicit val byteArrayPrinter = apply("Raw") { (_, v: Array[Byte]) =>
      val sb = new StringBuilder
      for (b <- v)
        sb.append(f"${b & 0xff}%02x").append(' ')
      sb.toString()
    }
  }

  class ShowContext(buf: Array[Byte], start: Int, end: Int, val options: ShowOptions) {
    private val sb = new StringBuilder
    private val sb2 = new StringBuilder
    private val stack = scala.collection.mutable.Stack[(Int, Int, String)]()
    private var offset = start
    private var firstInTuple = false
    private var indent = 0

    def show(): String = {
      import Packer._
      while (offset < end) {
        buf(offset) match {
          case TYPE_STRING => eatWith(of[String])
          case TYPE_UINT32 => eatWith(of[Int])
          case TYPE_UINT64 => eatWith(of[Long])
          case TYPE_TRUE | TYPE_FALSE => eatWith(of[Boolean])
          case TYPE_RAW => eatWith(of[Array[Byte]])
          case TYPE_TUPLE => tupleBegin(1)
          case TYPE_END => tupleEnd(1)
          case Range.TYPE_MAX => eat(1, "Max")
          case Range.TYPE_MIN => eat(1, "Max")
        }
      }
      sb.toString()
    }

    private def tokenBegin(tpe: String) = {
      stack.push((offset, sb.length, tpe))
      this
    }

    private def tokenEnd() = {
      val (start, sbStart, tpe) = stack.pop()
      val end = offset
      if (options.tpe) {
        sb2.append(tpe)
        if (options.offset)
          sb2.append(" : ").append(start).append(" : ").append(end)
        sb2.append(" -> ")
      } else if (options.offset) {
        sb2.append(start).append(" : ").append(end).append(" -> ")
      }
      sb.insert(sbStart, sb2.toString())
      sb2.clear()
    }

    private def eatWith[T](p: Packer[T])(implicit printer: ShowPrinter[T]): Unit = {
      val len = p.unpackLength(buf, offset)
      val v = p.unpack(buf, offset, len)
      eat[T](len, v)
    }

    private def eat[T](len: Int, v: T)(implicit printer: ShowPrinter[T]): Unit = {
      space()
      tokenBegin(printer.tpe)
      sb.append(printer(options, v))
      offset += len
      tokenEnd()
    }

    private def space(): Unit = {
      if (!stack.isEmpty) {
        if (firstInTuple)
          firstInTuple = false
        else
          sb.append(", ")
        if (options.wrap) {
          sb.append("\r\n")
          if (indent > 0) {
            var i = 0
            while (i < indent) {
              sb.append("  ")
              i += 1
            }
          }
        }
      }
    }

    private def tupleBegin(len: Int) = {
      space()
      tokenBegin("Tuple")
      sb.append("(")
      firstInTuple = true
      indent += 1
      offset += len
    }

    private def tupleEnd(len: Int) = {
      if (stack.isEmpty) {
        println("eee")
      }
      require(stack.top._3 == "Tuple")
      sb.append(")")
      firstInTuple = false
      offset += len
      indent -= 1
      tokenEnd()
    }

  }

}
