package com.weez.mercury.debug

import com.weez.mercury.common._

object DBDebugService extends RemoteService {
  val listEntityMetas: QueryCall = c => {
    import c._
    val prefix: String = request.prefix
    val cur =
      (if (prefix.length > 0) {
        EntityMetaCollection.byName(prefix.asPrefix)
      } else {
        EntityMetaCollection()
      }) map { m =>
        ModelObject("id" -> m.id,
          "name" -> m.name,
          "abstract" -> m.isAbstract,
          "toplevel" -> m.isTopLevel,
          "columns" -> m.columns.map { c =>
            ModelObject("name" -> c.name, "type" -> c.tpe.toString)
          },
          "parents" -> m.parents)
      }
    completeWithPager(cur, "id")
    cur.close()
  }

  val listCollectionMetas: QueryCall = c => {
    import c._
    val prefix: String = request.prefix
    val cur =
      (if (prefix.length > 0) {
        CollectionMetaCollection.byName(prefix.asPrefix)
      } else {
        CollectionMetaCollection()
      }) map { m =>
        ModelObject("id" -> m.id,
          "name" -> m.name,
          "root" -> m.isRoot,
          "valuetype" -> m.valueType.toString,
          "prefix" -> m.prefix,
          "indexes" -> m.indexes.map { i =>
            ModelObject("name" -> i.name,
              "keytype" -> i.key.toString,
              "unique" -> i.unique,
              "prefix" -> i.prefix)
          })
      }
    completeWithPager(cur, "id")
    cur.close()
  }

  val listRootCollection: QueryCall = c => {
    import c._
    val name: String = request.collectionName
    CollectionMetaCollection.byName(name) match {
      case Some(x) =>
        if (x.isRoot) {
          val options = PackerDebug.ShowOptions(wrap = true, offset = true, tpe = true)
          val mc = new MetaContext
          val prefix = (x.prefix & 0xffffL) << 48
          val range = prefix +-+ (prefix | (-1L >>> 16))
          val cur = new Cursor.RawCursor(range, true) map { buf =>
            val (v, _) = unpack(buf, 0, x.valueType, mc)
            v.asInstanceOf[ModelObject]
          }
          completeWithPager(cur, "id")
          cur.close()
        } else {
          failWith("not root collection")
        }
      case None =>
        failWith("not found")
    }
  }

  def unpack(buf: Array[Byte], offset: Int, ref: DBType.DBTypeRef, c: MetaContext): (Any, Int) = {
    import org.joda.time.DateTime
    ref match {
      case x: DBType.SimpleType =>
        val packer =
          x match {
            case DBType.String => Packer.of[String]
            case DBType.Int => Packer.of[Int]
            case DBType.Long => Packer.of[Long]
            case DBType.Double => Packer.of[Double]
            case DBType.DateTime => Packer.of[DateTime]
            case DBType.Boolean => Packer.of[Boolean]
            case DBType.Raw => Packer.of[Array[Byte]]
          }
        val len = packer.unpackLength(buf, offset)
        packer.unpack(buf, offset, len) -> (offset + len)
      case DBType.Tuple(x) =>
        require(buf(offset) == Packer.TYPE_TUPLE)
        var off = offset + 1
        val arr = x map { p =>
          val (elem, end) = unpack(buf, off, p, c)
          off = end
          elem
        }
        require(buf(off) == Packer.TYPE_END)
        arr -> (off + 1)
      case DBType.Coll(x) =>
        require(buf(offset) == Packer.TYPE_TUPLE)
        val builder = Seq.newBuilder[Any]
        var off = offset + 1
        while (buf(off) != Packer.TYPE_END) {
          val (elem, end) = unpack(buf, off, x, c)
          builder += elem
          off = end
        }
        builder.result() -> (off + 1)
      case DBType.EntityRef(x) =>
        val meta = c.getEntityMeta(x)
        if (meta.isAbstract) {
          unpack(buf, offset)
        } else {
          require(buf(offset) == Packer.TYPE_TUPLE)
          var off = offset + 1
          val cols = meta.columns map { col =>
            val (column, end) = unpack(buf, off, col.tpe, c)
            off = end
            col.name -> column
          }
          require(buf(off) == Packer.TYPE_END)
          ModelObject(cols: _*) -> (off + 1)
        }
      case DBType.EntityCollRef(x) =>
        ???
      case DBType.Ref(x) =>
        x.toString -> (Packer.of[Long].unpackLength(buf, offset) + offset)
    }
  }

  def unpack(buf: Array[Byte], offset: Int): (Any, Int) = {
    if (buf(offset) == Packer.TYPE_TUPLE) {
      val builder = Seq.newBuilder[(String, Any)]
      var off = offset + 1
      var i = 1
      while (buf(off) != Packer.TYPE_END) {
        val (v, end) = unpack(buf, off)
        builder += s"_$i" -> v
        i += 1
        off = end
      }
      require(buf(off) == Packer.TYPE_END)
      ModelObject(builder.result(): _*) -> (off + 1)
    } else {
      val packer =
        buf(offset) match {
          case Packer.TYPE_STRING => Packer.of[String]
          case Packer.TYPE_UINT32 => Packer.of[Int]
          case Packer.TYPE_UINT64 => Packer.of[Long]
          case Packer.TYPE_FALSE => Packer.of[Boolean]
          case Packer.TYPE_TRUE => Packer.of[Boolean]
          case Packer.TYPE_RAW => Packer.of[Array[Byte]]
        }
      val len = packer.unpackLength(buf, offset)
      val v = packer.unpack(buf, offset, len)
      v -> (offset + len)
    }
  }

  class MetaContext(implicit db: DBSessionQueryable) {

    import scala.collection.mutable

    private val entityMetas = mutable.Map[String, DBType.EntityMeta]()

    def getEntityMeta(name: String) = {
      entityMetas.getOrElseUpdate(name, {
        EntityMetaCollection.byName(name).getOrElse(throw new IllegalStateException())
      })
    }
  }

}
