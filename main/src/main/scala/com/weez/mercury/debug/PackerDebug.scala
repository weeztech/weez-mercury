package com.weez.mercury.debug

object PackerDebug {

  import com.weez.mercury.common._

  def show(buf: Array[Byte]): String = show(buf, 0, buf.length)

  def show(buf: Array[Byte], start: Int, end: Int): String = {
    def eat(offset: Int): (String, Int) = {
      import Packer._
      buf(offset) match {
        case TYPE_STRING =>
          val len = StringPacker.unpackLength(buf, offset)
          val v = StringPacker.unpack(buf, offset, len)
          Util.showString(v) -> (offset + len)
        case TYPE_UINT32 =>
          val len = IntPacker.unpackLength(buf, offset)
          val v = IntPacker.unpack(buf, offset, len)
          v.toString -> (offset + len)
        case TYPE_UINT64 =>
          val len = LongPacker.unpackLength(buf, offset)
          val v = LongPacker.unpack(buf, offset, len)
          (v.toString + "L") -> (offset + len)
        case TYPE_TRUE | TYPE_FALSE =>
          val len = BooleanPacker.unpackLength(buf, offset)
          val v = BooleanPacker.unpack(buf, offset, len)
          v.toString -> (offset + len)
        case TYPE_RAW =>
          val len = ByteArrayPacker.unpackLength(buf, offset)
          val v = ByteArrayPacker.unpack(buf, offset, len)
          Util.showHex(v) -> (offset + len)
        case TYPE_TUPLE =>
          val sb = new StringBuilder
          sb.append('(')
          var off = offset + 1
          if (buf(off) != TYPE_END) {
            val (s, end) = eat(off)
            sb.append(s)
            off = end
            while (buf(off) != TYPE_END) {
              val (s, end) = eat(off)
              sb.append(", ").append(s)
              off = end
            }
          }
          sb.append(')')
          sb.toString() -> (off + 1)
        case Range.TYPE_MAX =>
          "Max" -> (offset + 1)
        case Range.TYPE_MIN =>
          "Min" -> (offset + 1)
      }
    }
    val (s, x) = eat(start)
    require(x == end, {
      val m = "invalid length"
      m
    })
    s
  }
}
