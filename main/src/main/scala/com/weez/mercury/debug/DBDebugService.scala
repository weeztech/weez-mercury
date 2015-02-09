package com.weez.mercury.debug

import com.weez.mercury.common._

object DBDebugService extends RemoteService {
  val listStructMetas: QueryCall = c => {
    import c._
    val prefix: String = request.prefix
    val cur =
      (if (prefix.length > 0) {
        MetaCollection.byName(prefix +-- prefix.prefixed)
      } else {
        MetaCollection()
      }) collect {
        case m: DBType.StructMeta =>
          ModelObject("id" -> m.id,
            "name" -> m.name,
            "isEntity" -> m.isEntity,
            "columns" -> m.columns.map { c =>
              ModelObject("name" -> c.name, "type" -> c.tpe.toString)
            },
            "interfaces" -> m.interfaces)
      }
    completeWithPager(cur, "id")
    cur.close()
  }

  val listCollectionMetas: QueryCall = c => {
    import c._
    val prefix: String = request.prefix
    val cur =
      (if (prefix.length > 0) {
        MetaCollection.byName(prefix +-- prefix.prefixed)
      } else {
        MetaCollection()
      }) collect {
        case m: DBType.CollectionMeta =>
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
    MetaCollection.byName(name) match {
      case Some(x: DBType.CollectionMeta) =>
        if (x.isRoot) {
          val prefix = (x.prefix & 0xffffL) << 48
          val range = prefix +-+ (prefix | (-1L >>> 16))
          val cur = new Cursor.RawCursor(range, true) map { buf =>
            val (v, _) = unpack(buf, 0, x.valueType)
            v.asInstanceOf[ModelObject]
          }
          completeWithPager(cur, "id")
          cur.close()
        } else {
          failWith("not root collection")
        }
      case _ =>
        failWith("not found")
    }
  }

  def unpack(buf: Array[Byte], offset: Int, ref: DBType.DBTypeRef)(implicit db: DBSessionQueryable): (Any, Int) = {
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
          val (elem, end) = unpack(buf, off, p)
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
          val (elem, end) = unpack(buf, off, x)
          builder += elem
          off = end
        }
        builder.result() -> (off + 1)
      case DBType.Struct(x) =>
        def withMeta(meta: DBType.StructMeta, buf: Array[Byte], offset: Int) = {
          require(buf(offset) == Packer.TYPE_TUPLE)
          var off = offset + 1
          val cols = meta.columns map { col =>
            val (column, end) = unpack(buf, off, col.tpe)
            off = end
            col.name -> column
          }
          require(buf(off) == Packer.TYPE_END)
          ModelObject(cols: _*) -> (off + 1)
        }

        MetaCollection.byName(x) match {
          case Some(m: DBType.StructMeta) => withMeta(m, buf, offset)
          case Some(m: DBType.InterfaceMeta) =>
            require(buf(offset) == Packer.TYPE_TUPLE)
            val subs = getFinalStructs(m)
            val p = Packer.of[String]
            var off = offset + 1
            val len = p.unpackLength(buf, off)
            val tpe = p.unpack(buf, off, len)
            off += len
            val v =
              subs(tpe) match {
                case x: DBType.StructMeta =>
                  val (v, end) = withMeta(x, buf, off)
                  off = end
                  v
                case x: DBType.ValueMeta =>
                  ModelObject("$value" -> x.name)
                case _ => throw new IllegalStateException()
              }
            require(buf(off) == Packer.TYPE_END)
            v -> (off + 1)
          case _ => throw new IllegalStateException()
        }
      case DBType.Ref(x) =>
        x.toString -> (Packer.of[Long].unpackLength(buf, offset) + offset)
    }
  }

  def getFinalStructs(tpe: DBType.InterfaceMeta)(implicit db: DBSessionQueryable): Map[String, DBType.Meta] = {
    val builder = Map.newBuilder[String, DBType.Meta]
    val queue = scala.collection.mutable.Queue[DBType.InterfaceMeta]()
    queue.enqueue(tpe)
    while (queue.nonEmpty) {
      val m = queue.dequeue()
      m.subs foreach { name =>
        MetaCollection.byName(name) match {
          case Some(x: DBType.InterfaceMeta) => queue.enqueue(x)
          case Some(x: DBType.StructMeta) => builder += x.name -> x
          case Some(x: DBType.ValueMeta) => builder += x.name -> x
          case _ => throw new IllegalStateException()
        }
      }
    }
    builder.result()
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
}
