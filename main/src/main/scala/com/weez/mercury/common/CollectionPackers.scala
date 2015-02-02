package com.weez.mercury.common

trait CollectionPackers {

  import Packer.{TYPE_TUPLE, TYPE_END}

  import scala.language.higherKinds
  import scala.language.implicitConversions
  import scala.collection._
  import scala.collection.generic._

  class CollectionPacker[X, CC[A] <: GenTraversable[A]](companion: GenericCompanion[CC])(implicit packer: Packer[X]) extends RawPacker[CC[X]] {
    def pack(value: CC[X], buf: Array[Byte], offset: Int) = {
      buf(offset) = TYPE_TUPLE
      var end = offset + 1
      value foreach { v =>
        packer.pack(v, buf, end)
        end += packer.packLength(v)
      }
      buf(end) = 0
      end + 1
    }

    def packLength(value: CC[X]): Int = {
      value.foldLeft(2) { (c, v) =>
        c + packer.packLength(v)
      }
    }

    def unpack(buf: Array[Byte], offset: Int, length: Int) = {
      require(buf(offset) == TYPE_TUPLE, "not a Tuple")
      var end = offset + 1
      val builder = companion.newBuilder[X]
      while (buf(end) != TYPE_END) {
        val len = packer.unpackLength(buf, end)
        builder += packer.unpack(buf, end, len)
        end += len
      }
      require(end + 1 == offset + length, "invalid length")
      builder.result()
    }

    def unpackLength(buf: Array[Byte], offset: Int) = {
      var end = offset + 1
      while (buf(end) != TYPE_END) {
        end += packer.unpackLength(buf, end)
      }
      end + 1 - offset
    }
  }

  implicit def imp_seq[T](implicit packer: Packer[T]): Packer[Seq[T]] = new CollectionPacker(Seq)

  implicit def imp_iseq[T](implicit packer: Packer[T]): Packer[immutable.Seq[T]] = new CollectionPacker(immutable.Seq)

  implicit def imp_mseq[T](implicit packer: Packer[T]): Packer[mutable.Seq[T]] = new CollectionPacker(mutable.Seq)

  implicit def imp_traversable[T](implicit packer: Packer[T]): Packer[Traversable[T]] = new CollectionPacker(Traversable)

  implicit def imp_itraversable[T](implicit packer: Packer[T]): Packer[immutable.Traversable[T]] = new CollectionPacker(immutable.Traversable)

  implicit def imp_mtraversable[T](implicit packer: Packer[T]): Packer[mutable.Traversable[T]] = new CollectionPacker(mutable.Traversable)

  implicit def imp_iterable[T](implicit packer: Packer[T]): Packer[Iterable[T]] = new CollectionPacker(Iterable)

  implicit def imp_iiterable[T](implicit packer: Packer[T]): Packer[immutable.Iterable[T]] = new CollectionPacker(immutable.Iterable)

  implicit def imp_miterable[T](implicit packer: Packer[T]): Packer[mutable.Iterable[T]] = new CollectionPacker(mutable.Iterable)

  implicit def imp_list[T](implicit packer: Packer[T]): Packer[List[T]] = new CollectionPacker(List)

  implicit def imp_arrayBuffer[T](implicit packer: Packer[T]): Packer[mutable.ArrayBuffer[T]] = new CollectionPacker(mutable.ArrayBuffer)

  implicit def imp_listBuffer[T](implicit packer: Packer[T]): Packer[mutable.ListBuffer[T]] = new CollectionPacker(mutable.ListBuffer)

  implicit def imp_map[A, B](implicit packer: Packer[(A, B)]): Packer[Map[A, B]] = Packer.map[Map[A, B], Traversable[(A, B)]](m => m, t => t.toMap)

  implicit def imp_imap[A, B](implicit packer: Packer[(A, B)]): Packer[immutable.Map[A, B]] = Packer.map[immutable.Map[A, B], immutable.Traversable[(A, B)]](m => m, t => t.toMap)

  implicit def imp_mmap[A, B](implicit packer: Packer[(A, B)]): Packer[mutable.Map[A, B]] =
    Packer.map[mutable.Map[A, B], mutable.Traversable[(A, B)]](m => m,
      t => {
        val b = mutable.Map.newBuilder[A, B]
        b ++= t
        b.result()
      })
}