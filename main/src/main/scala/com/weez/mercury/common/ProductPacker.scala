package com.weez.mercury.common

import scala.language.implicitConversions

trait ProductPackers {

  import Packer.TYPE_TUPLE

  class ProductPacker[T <: Product](packers: Packer[_]*)(f: Seq[Any] => T) extends Packer[T] {
    val _packers = packers.asInstanceOf[Seq[Packer[Any]]]

    def pack(value: T, buf: Array[Byte], offset: Int) = {
      buf(offset) = TYPE_TUPLE
      var itor = value.productIterator
      var end = offset + 1
      _packers foreach { p =>
        val v = itor.next()
        p.pack(v, buf, end)
        end += p.packLength(v)
      }
      buf(end) = 0
    }

    def packLength(value: T): Int = {
      var itor = value.productIterator
      _packers.foldLeft(2) { (c, p) =>
        c + p.packLength(itor.next())
      }
    }

    def unpack(buf: Array[Byte], offset: Int, length: Int) = {
      require(buf(offset) == TYPE_TUPLE, "not a Tuple")
      var end = offset + 1
      val args = packers map { p =>
        val len = p.unpackLength(buf, end)
        val v = p.unpack(buf, end, len)
        end += len
        v
      }
      require(end + 1 - offset == length, "invalid length")
      f(args)
    }

    def unpackLength(buf: Array[Byte], offset: Int) = {
      val end = packers.foldLeft(offset + 1) { (end, p) =>
        end + p.unpackLength(buf, end)
      }
      end + 1 - offset
    }
  }

  implicit def tuple1[A](implicit p0: Packer[A]): Packer[Tuple1[A]] =
    new ProductPacker[Tuple1[A]](p0)(a => Tuple1(a(0).asInstanceOf[A]))

  implicit def tuple2[A, B](implicit p0: Packer[A], p1: Packer[B]): Packer[(A, B)] =
    new ProductPacker[(A, B)](p0, p1)(a => (a(0).asInstanceOf[A], a(1).asInstanceOf[B]))

  implicit def tuple3[A, B, C](implicit p0: Packer[A], p1: Packer[B], p2: Packer[C]): Packer[(A, B, C)] =
    new ProductPacker[(A, B, C)](p0, p1, p2)(a => (a(0).asInstanceOf[A], a(1).asInstanceOf[B], a(2).asInstanceOf[C]))

  implicit def tuple4[A, B, C, D](implicit p0: Packer[A], p1: Packer[B], p2: Packer[C], p3: Packer[D]): Packer[(A, B, C, D)] =
    new ProductPacker[(A, B, C, D)](p0, p1, p2, p3)(a => (a(0).asInstanceOf[A], a(1).asInstanceOf[B], a(2).asInstanceOf[C], a(3).asInstanceOf[D]))

  implicit def tuple5[A, B, C, D, E](implicit p0: Packer[A], p1: Packer[B], p2: Packer[C], p3: Packer[D], p4: Packer[E]): Packer[(A, B, C, D, E)] =
    new ProductPacker[(A, B, C, D, E)](p0, p1, p2, p3, p4)(a => (a(0).asInstanceOf[A], a(1).asInstanceOf[B], a(2).asInstanceOf[C], a(3).asInstanceOf[D], a(4).asInstanceOf[E]))

  implicit def tuple6[A, B, C, D, E, F](implicit p0: Packer[A], p1: Packer[B], p2: Packer[C], p3: Packer[D], p4: Packer[E], p5: Packer[F]): Packer[(A, B, C, D, E, F)] =
    new ProductPacker[(A, B, C, D, E, F)](p0, p1, p2, p3, p4, p5)(a => (a(0).asInstanceOf[A], a(1).asInstanceOf[B], a(2).asInstanceOf[C], a(3).asInstanceOf[D], a(4).asInstanceOf[E], a(5).asInstanceOf[F]))

  implicit def tuple7[A, B, C, D, E, F, G](implicit p0: Packer[A], p1: Packer[B], p2: Packer[C], p3: Packer[D], p4: Packer[E], p5: Packer[F], p6: Packer[G]): Packer[(A, B, C, D, E, F, G)] =
    new ProductPacker[(A, B, C, D, E, F, G)](p0, p1, p2, p3, p4, p5, p6)(a => (a(0).asInstanceOf[A], a(1).asInstanceOf[B], a(2).asInstanceOf[C], a(3).asInstanceOf[D], a(4).asInstanceOf[E], a(5).asInstanceOf[F], a(6).asInstanceOf[G]))

  implicit def tuple8[A, B, C, D, E, F, G, H](implicit p0: Packer[A], p1: Packer[B], p2: Packer[C], p3: Packer[D], p4: Packer[E], p5: Packer[F], p6: Packer[G], p7: Packer[H]): Packer[(A, B, C, D, E, F, G, H)] =
    new ProductPacker[(A, B, C, D, E, F, G, H)](p0, p1, p2, p3, p4, p5, p6, p7)(a => (a(0).asInstanceOf[A], a(1).asInstanceOf[B], a(2).asInstanceOf[C], a(3).asInstanceOf[D], a(4).asInstanceOf[E], a(5).asInstanceOf[F], a(6).asInstanceOf[G], a(7).asInstanceOf[H]))

  implicit def tuple9[A, B, C, D, E, F, G, H, I](implicit p0: Packer[A], p1: Packer[B], p2: Packer[C], p3: Packer[D], p4: Packer[E], p5: Packer[F], p6: Packer[G], p7: Packer[H], p8: Packer[I]): Packer[(A, B, C, D, E, F, G, H, I)] =
    new ProductPacker[(A, B, C, D, E, F, G, H, I)](p0, p1, p2, p3, p4, p5, p6, p7, p8)(a => (a(0).asInstanceOf[A], a(1).asInstanceOf[B], a(2).asInstanceOf[C], a(3).asInstanceOf[D], a(4).asInstanceOf[E], a(5).asInstanceOf[F], a(6).asInstanceOf[G], a(7).asInstanceOf[H], a(8).asInstanceOf[I]))

  implicit def tuple10[A, B, C, D, E, F, G, H, I, J](implicit p0: Packer[A], p1: Packer[B], p2: Packer[C], p3: Packer[D], p4: Packer[E], p5: Packer[F], p6: Packer[G], p7: Packer[H], p8: Packer[I], p9: Packer[J]): Packer[(A, B, C, D, E, F, G, H, I, J)] =
    new ProductPacker[(A, B, C, D, E, F, G, H, I, J)](p0, p1, p2, p3, p4, p5, p6, p7, p8, p9)(a => (a(0).asInstanceOf[A], a(1).asInstanceOf[B], a(2).asInstanceOf[C], a(3).asInstanceOf[D], a(4).asInstanceOf[E], a(5).asInstanceOf[F], a(6).asInstanceOf[G], a(7).asInstanceOf[H], a(8).asInstanceOf[I], a(9).asInstanceOf[J]))

  implicit def tuple11[A, B, C, D, E, F, G, H, I, J, K](implicit p0: Packer[A], p1: Packer[B], p2: Packer[C], p3: Packer[D], p4: Packer[E], p5: Packer[F], p6: Packer[G], p7: Packer[H], p8: Packer[I], p9: Packer[J], p10: Packer[K]): Packer[(A, B, C, D, E, F, G, H, I, J, K)] =
    new ProductPacker[(A, B, C, D, E, F, G, H, I, J, K)](p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10)(a => (a(0).asInstanceOf[A], a(1).asInstanceOf[B], a(2).asInstanceOf[C], a(3).asInstanceOf[D], a(4).asInstanceOf[E], a(5).asInstanceOf[F], a(6).asInstanceOf[G], a(7).asInstanceOf[H], a(8).asInstanceOf[I], a(9).asInstanceOf[J], a(10).asInstanceOf[K]))

  implicit def tuple12[A, B, C, D, E, F, G, H, I, J, K, L](implicit p0: Packer[A], p1: Packer[B], p2: Packer[C], p3: Packer[D], p4: Packer[E], p5: Packer[F], p6: Packer[G], p7: Packer[H], p8: Packer[I], p9: Packer[J], p10: Packer[K], p11: Packer[L]): Packer[(A, B, C, D, E, F, G, H, I, J, K, L)] =
    new ProductPacker[(A, B, C, D, E, F, G, H, I, J, K, L)](p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11)(a => (a(0).asInstanceOf[A], a(1).asInstanceOf[B], a(2).asInstanceOf[C], a(3).asInstanceOf[D], a(4).asInstanceOf[E], a(5).asInstanceOf[F], a(6).asInstanceOf[G], a(7).asInstanceOf[H], a(8).asInstanceOf[I], a(9).asInstanceOf[J], a(10).asInstanceOf[K], a(11).asInstanceOf[L]))

  implicit def tuple13[A, B, C, D, E, F, G, H, I, J, K, L, M](implicit p0: Packer[A], p1: Packer[B], p2: Packer[C], p3: Packer[D], p4: Packer[E], p5: Packer[F], p6: Packer[G], p7: Packer[H], p8: Packer[I], p9: Packer[J], p10: Packer[K], p11: Packer[L], p12: Packer[M]): Packer[(A, B, C, D, E, F, G, H, I, J, K, L, M)] =
    new ProductPacker[(A, B, C, D, E, F, G, H, I, J, K, L, M)](p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12)(a => (a(0).asInstanceOf[A], a(1).asInstanceOf[B], a(2).asInstanceOf[C], a(3).asInstanceOf[D], a(4).asInstanceOf[E], a(5).asInstanceOf[F], a(6).asInstanceOf[G], a(7).asInstanceOf[H], a(8).asInstanceOf[I], a(9).asInstanceOf[J], a(10).asInstanceOf[K], a(11).asInstanceOf[L], a(12).asInstanceOf[M]))

  implicit def tuple14[A, B, C, D, E, F, G, H, I, J, K, L, M, N](implicit p0: Packer[A], p1: Packer[B], p2: Packer[C], p3: Packer[D], p4: Packer[E], p5: Packer[F], p6: Packer[G], p7: Packer[H], p8: Packer[I], p9: Packer[J], p10: Packer[K], p11: Packer[L], p12: Packer[M], p13: Packer[N]): Packer[(A, B, C, D, E, F, G, H, I, J, K, L, M, N)] =
    new ProductPacker[(A, B, C, D, E, F, G, H, I, J, K, L, M, N)](p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13)(a => (a(0).asInstanceOf[A], a(1).asInstanceOf[B], a(2).asInstanceOf[C], a(3).asInstanceOf[D], a(4).asInstanceOf[E], a(5).asInstanceOf[F], a(6).asInstanceOf[G], a(7).asInstanceOf[H], a(8).asInstanceOf[I], a(9).asInstanceOf[J], a(10).asInstanceOf[K], a(11).asInstanceOf[L], a(12).asInstanceOf[M], a(13).asInstanceOf[N]))

  implicit def tuple15[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O](implicit p0: Packer[A], p1: Packer[B], p2: Packer[C], p3: Packer[D], p4: Packer[E], p5: Packer[F], p6: Packer[G], p7: Packer[H], p8: Packer[I], p9: Packer[J], p10: Packer[K], p11: Packer[L], p12: Packer[M], p13: Packer[N], p14: Packer[O]): Packer[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O)] =
    new ProductPacker[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O)](p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14)(a => (a(0).asInstanceOf[A], a(1).asInstanceOf[B], a(2).asInstanceOf[C], a(3).asInstanceOf[D], a(4).asInstanceOf[E], a(5).asInstanceOf[F], a(6).asInstanceOf[G], a(7).asInstanceOf[H], a(8).asInstanceOf[I], a(9).asInstanceOf[J], a(10).asInstanceOf[K], a(11).asInstanceOf[L], a(12).asInstanceOf[M], a(13).asInstanceOf[N], a(14).asInstanceOf[O]))

  implicit def tuple16[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P](implicit p0: Packer[A], p1: Packer[B], p2: Packer[C], p3: Packer[D], p4: Packer[E], p5: Packer[F], p6: Packer[G], p7: Packer[H], p8: Packer[I], p9: Packer[J], p10: Packer[K], p11: Packer[L], p12: Packer[M], p13: Packer[N], p14: Packer[O], p15: Packer[P]): Packer[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P)] =
    new ProductPacker[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P)](p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15)(a => (a(0).asInstanceOf[A], a(1).asInstanceOf[B], a(2).asInstanceOf[C], a(3).asInstanceOf[D], a(4).asInstanceOf[E], a(5).asInstanceOf[F], a(6).asInstanceOf[G], a(7).asInstanceOf[H], a(8).asInstanceOf[I], a(9).asInstanceOf[J], a(10).asInstanceOf[K], a(11).asInstanceOf[L], a(12).asInstanceOf[M], a(13).asInstanceOf[N], a(14).asInstanceOf[O], a(15).asInstanceOf[P]))

  implicit def tuple17[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q](implicit p0: Packer[A], p1: Packer[B], p2: Packer[C], p3: Packer[D], p4: Packer[E], p5: Packer[F], p6: Packer[G], p7: Packer[H], p8: Packer[I], p9: Packer[J], p10: Packer[K], p11: Packer[L], p12: Packer[M], p13: Packer[N], p14: Packer[O], p15: Packer[P], p16: Packer[Q]): Packer[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q)] =
    new ProductPacker[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q)](p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16)(a => (a(0).asInstanceOf[A], a(1).asInstanceOf[B], a(2).asInstanceOf[C], a(3).asInstanceOf[D], a(4).asInstanceOf[E], a(5).asInstanceOf[F], a(6).asInstanceOf[G], a(7).asInstanceOf[H], a(8).asInstanceOf[I], a(9).asInstanceOf[J], a(10).asInstanceOf[K], a(11).asInstanceOf[L], a(12).asInstanceOf[M], a(13).asInstanceOf[N], a(14).asInstanceOf[O], a(15).asInstanceOf[P], a(16).asInstanceOf[Q]))

  implicit def tuple18[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R](implicit p0: Packer[A], p1: Packer[B], p2: Packer[C], p3: Packer[D], p4: Packer[E], p5: Packer[F], p6: Packer[G], p7: Packer[H], p8: Packer[I], p9: Packer[J], p10: Packer[K], p11: Packer[L], p12: Packer[M], p13: Packer[N], p14: Packer[O], p15: Packer[P], p16: Packer[Q], p17: Packer[R]): Packer[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R)] =
    new ProductPacker[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R)](p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17)(a => (a(0).asInstanceOf[A], a(1).asInstanceOf[B], a(2).asInstanceOf[C], a(3).asInstanceOf[D], a(4).asInstanceOf[E], a(5).asInstanceOf[F], a(6).asInstanceOf[G], a(7).asInstanceOf[H], a(8).asInstanceOf[I], a(9).asInstanceOf[J], a(10).asInstanceOf[K], a(11).asInstanceOf[L], a(12).asInstanceOf[M], a(13).asInstanceOf[N], a(14).asInstanceOf[O], a(15).asInstanceOf[P], a(16).asInstanceOf[Q], a(17).asInstanceOf[R]))

  implicit def tuple19[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S](implicit p0: Packer[A], p1: Packer[B], p2: Packer[C], p3: Packer[D], p4: Packer[E], p5: Packer[F], p6: Packer[G], p7: Packer[H], p8: Packer[I], p9: Packer[J], p10: Packer[K], p11: Packer[L], p12: Packer[M], p13: Packer[N], p14: Packer[O], p15: Packer[P], p16: Packer[Q], p17: Packer[R], p18: Packer[S]): Packer[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S)] =
    new ProductPacker[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S)](p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18)(a => (a(0).asInstanceOf[A], a(1).asInstanceOf[B], a(2).asInstanceOf[C], a(3).asInstanceOf[D], a(4).asInstanceOf[E], a(5).asInstanceOf[F], a(6).asInstanceOf[G], a(7).asInstanceOf[H], a(8).asInstanceOf[I], a(9).asInstanceOf[J], a(10).asInstanceOf[K], a(11).asInstanceOf[L], a(12).asInstanceOf[M], a(13).asInstanceOf[N], a(14).asInstanceOf[O], a(15).asInstanceOf[P], a(16).asInstanceOf[Q], a(17).asInstanceOf[R], a(18).asInstanceOf[S]))

  implicit def tuple20[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T](implicit p0: Packer[A], p1: Packer[B], p2: Packer[C], p3: Packer[D], p4: Packer[E], p5: Packer[F], p6: Packer[G], p7: Packer[H], p8: Packer[I], p9: Packer[J], p10: Packer[K], p11: Packer[L], p12: Packer[M], p13: Packer[N], p14: Packer[O], p15: Packer[P], p16: Packer[Q], p17: Packer[R], p18: Packer[S], p19: Packer[T]): Packer[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T)] =
    new ProductPacker[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T)](p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18, p19)(a => (a(0).asInstanceOf[A], a(1).asInstanceOf[B], a(2).asInstanceOf[C], a(3).asInstanceOf[D], a(4).asInstanceOf[E], a(5).asInstanceOf[F], a(6).asInstanceOf[G], a(7).asInstanceOf[H], a(8).asInstanceOf[I], a(9).asInstanceOf[J], a(10).asInstanceOf[K], a(11).asInstanceOf[L], a(12).asInstanceOf[M], a(13).asInstanceOf[N], a(14).asInstanceOf[O], a(15).asInstanceOf[P], a(16).asInstanceOf[Q], a(17).asInstanceOf[R], a(18).asInstanceOf[S], a(19).asInstanceOf[T]))

  implicit def tuple21[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U](implicit p0: Packer[A], p1: Packer[B], p2: Packer[C], p3: Packer[D], p4: Packer[E], p5: Packer[F], p6: Packer[G], p7: Packer[H], p8: Packer[I], p9: Packer[J], p10: Packer[K], p11: Packer[L], p12: Packer[M], p13: Packer[N], p14: Packer[O], p15: Packer[P], p16: Packer[Q], p17: Packer[R], p18: Packer[S], p19: Packer[T], p20: Packer[U]): Packer[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U)] =
    new ProductPacker[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U)](p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18, p19, p20)(a => (a(0).asInstanceOf[A], a(1).asInstanceOf[B], a(2).asInstanceOf[C], a(3).asInstanceOf[D], a(4).asInstanceOf[E], a(5).asInstanceOf[F], a(6).asInstanceOf[G], a(7).asInstanceOf[H], a(8).asInstanceOf[I], a(9).asInstanceOf[J], a(10).asInstanceOf[K], a(11).asInstanceOf[L], a(12).asInstanceOf[M], a(13).asInstanceOf[N], a(14).asInstanceOf[O], a(15).asInstanceOf[P], a(16).asInstanceOf[Q], a(17).asInstanceOf[R], a(18).asInstanceOf[S], a(19).asInstanceOf[T], a(20).asInstanceOf[U]))

  implicit def tuple22[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V](implicit p0: Packer[A], p1: Packer[B], p2: Packer[C], p3: Packer[D], p4: Packer[E], p5: Packer[F], p6: Packer[G], p7: Packer[H], p8: Packer[I], p9: Packer[J], p10: Packer[K], p11: Packer[L], p12: Packer[M], p13: Packer[N], p14: Packer[O], p15: Packer[P], p16: Packer[Q], p17: Packer[R], p18: Packer[S], p19: Packer[T], p20: Packer[U], p21: Packer[V]): Packer[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V)] =
    new ProductPacker[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V)](p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18, p19, p20, p21)(a => (a(0).asInstanceOf[A], a(1).asInstanceOf[B], a(2).asInstanceOf[C], a(3).asInstanceOf[D], a(4).asInstanceOf[E], a(5).asInstanceOf[F], a(6).asInstanceOf[G], a(7).asInstanceOf[H], a(8).asInstanceOf[I], a(9).asInstanceOf[J], a(10).asInstanceOf[K], a(11).asInstanceOf[L], a(12).asInstanceOf[M], a(13).asInstanceOf[N], a(14).asInstanceOf[O], a(15).asInstanceOf[P], a(16).asInstanceOf[Q], a(17).asInstanceOf[R], a(18).asInstanceOf[S], a(19).asInstanceOf[T], a(20).asInstanceOf[U], a(21).asInstanceOf[V]))

  def apply[Z <: Product, A](f: (A) => Z)(implicit p0: Packer[A]): Packer[Z] =
    new ProductPacker[Z](p0)(a => f(a(0).asInstanceOf[A]))

  def apply[Z <: Product, A, B](f: (A, B) => Z)(implicit p0: Packer[A], p1: Packer[B]): Packer[Z] =
    new ProductPacker[Z](p0, p1)(a => f(a(0).asInstanceOf[A], a(1).asInstanceOf[B]))

  def apply[Z <: Product, A, B, C](f: (A, B, C) => Z)(implicit p0: Packer[A], p1: Packer[B], p2: Packer[C]): Packer[Z] =
    new ProductPacker[Z](p0, p1, p2)(a => f(a(0).asInstanceOf[A], a(1).asInstanceOf[B], a(2).asInstanceOf[C]))

  def apply[Z <: Product, A, B, C, D](f: (A, B, C, D) => Z)(implicit p0: Packer[A], p1: Packer[B], p2: Packer[C], p3: Packer[D]): Packer[Z] =
    new ProductPacker[Z](p0, p1, p2, p3)(a => f(a(0).asInstanceOf[A], a(1).asInstanceOf[B], a(2).asInstanceOf[C], a(3).asInstanceOf[D]))

  def apply[Z <: Product, A, B, C, D, E](f: (A, B, C, D, E) => Z)(implicit p0: Packer[A], p1: Packer[B], p2: Packer[C], p3: Packer[D], p4: Packer[E]): Packer[Z] =
    new ProductPacker[Z](p0, p1, p2, p3, p4)(a => f(a(0).asInstanceOf[A], a(1).asInstanceOf[B], a(2).asInstanceOf[C], a(3).asInstanceOf[D], a(4).asInstanceOf[E]))

  def apply[Z <: Product, A, B, C, D, E, F](f: (A, B, C, D, E, F) => Z)(implicit p0: Packer[A], p1: Packer[B], p2: Packer[C], p3: Packer[D], p4: Packer[E], p5: Packer[F]): Packer[Z] =
    new ProductPacker[Z](p0, p1, p2, p3, p4, p5)(a => f(a(0).asInstanceOf[A], a(1).asInstanceOf[B], a(2).asInstanceOf[C], a(3).asInstanceOf[D], a(4).asInstanceOf[E], a(5).asInstanceOf[F]))

  def apply[Z <: Product, A, B, C, D, E, F, G](f: (A, B, C, D, E, F, G) => Z)(implicit p0: Packer[A], p1: Packer[B], p2: Packer[C], p3: Packer[D], p4: Packer[E], p5: Packer[F], p6: Packer[G]): Packer[Z] =
    new ProductPacker[Z](p0, p1, p2, p3, p4, p5, p6)(a => f(a(0).asInstanceOf[A], a(1).asInstanceOf[B], a(2).asInstanceOf[C], a(3).asInstanceOf[D], a(4).asInstanceOf[E], a(5).asInstanceOf[F], a(6).asInstanceOf[G]))

  def apply[Z <: Product, A, B, C, D, E, F, G, H](f: (A, B, C, D, E, F, G, H) => Z)(implicit p0: Packer[A], p1: Packer[B], p2: Packer[C], p3: Packer[D], p4: Packer[E], p5: Packer[F], p6: Packer[G], p7: Packer[H]): Packer[Z] =
    new ProductPacker[Z](p0, p1, p2, p3, p4, p5, p6, p7)(a => f(a(0).asInstanceOf[A], a(1).asInstanceOf[B], a(2).asInstanceOf[C], a(3).asInstanceOf[D], a(4).asInstanceOf[E], a(5).asInstanceOf[F], a(6).asInstanceOf[G], a(7).asInstanceOf[H]))

  def apply[Z <: Product, A, B, C, D, E, F, G, H, I](f: (A, B, C, D, E, F, G, H, I) => Z)(implicit p0: Packer[A], p1: Packer[B], p2: Packer[C], p3: Packer[D], p4: Packer[E], p5: Packer[F], p6: Packer[G], p7: Packer[H], p8: Packer[I]): Packer[Z] =
    new ProductPacker[Z](p0, p1, p2, p3, p4, p5, p6, p7, p8)(a => f(a(0).asInstanceOf[A], a(1).asInstanceOf[B], a(2).asInstanceOf[C], a(3).asInstanceOf[D], a(4).asInstanceOf[E], a(5).asInstanceOf[F], a(6).asInstanceOf[G], a(7).asInstanceOf[H], a(8).asInstanceOf[I]))

  def apply[Z <: Product, A, B, C, D, E, F, G, H, I, J](f: (A, B, C, D, E, F, G, H, I, J) => Z)(implicit p0: Packer[A], p1: Packer[B], p2: Packer[C], p3: Packer[D], p4: Packer[E], p5: Packer[F], p6: Packer[G], p7: Packer[H], p8: Packer[I], p9: Packer[J]): Packer[Z] =
    new ProductPacker[Z](p0, p1, p2, p3, p4, p5, p6, p7, p8, p9)(a => f(a(0).asInstanceOf[A], a(1).asInstanceOf[B], a(2).asInstanceOf[C], a(3).asInstanceOf[D], a(4).asInstanceOf[E], a(5).asInstanceOf[F], a(6).asInstanceOf[G], a(7).asInstanceOf[H], a(8).asInstanceOf[I], a(9).asInstanceOf[J]))

  def apply[Z <: Product, A, B, C, D, E, F, G, H, I, J, K](f: (A, B, C, D, E, F, G, H, I, J, K) => Z)(implicit p0: Packer[A], p1: Packer[B], p2: Packer[C], p3: Packer[D], p4: Packer[E], p5: Packer[F], p6: Packer[G], p7: Packer[H], p8: Packer[I], p9: Packer[J], p10: Packer[K]): Packer[Z] =
    new ProductPacker[Z](p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10)(a => f(a(0).asInstanceOf[A], a(1).asInstanceOf[B], a(2).asInstanceOf[C], a(3).asInstanceOf[D], a(4).asInstanceOf[E], a(5).asInstanceOf[F], a(6).asInstanceOf[G], a(7).asInstanceOf[H], a(8).asInstanceOf[I], a(9).asInstanceOf[J], a(10).asInstanceOf[K]))

  def apply[Z <: Product, A, B, C, D, E, F, G, H, I, J, K, L](f: (A, B, C, D, E, F, G, H, I, J, K, L) => Z)(implicit p0: Packer[A], p1: Packer[B], p2: Packer[C], p3: Packer[D], p4: Packer[E], p5: Packer[F], p6: Packer[G], p7: Packer[H], p8: Packer[I], p9: Packer[J], p10: Packer[K], p11: Packer[L]): Packer[Z] =
    new ProductPacker[Z](p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11)(a => f(a(0).asInstanceOf[A], a(1).asInstanceOf[B], a(2).asInstanceOf[C], a(3).asInstanceOf[D], a(4).asInstanceOf[E], a(5).asInstanceOf[F], a(6).asInstanceOf[G], a(7).asInstanceOf[H], a(8).asInstanceOf[I], a(9).asInstanceOf[J], a(10).asInstanceOf[K], a(11).asInstanceOf[L]))

  def apply[Z <: Product, A, B, C, D, E, F, G, H, I, J, K, L, M](f: (A, B, C, D, E, F, G, H, I, J, K, L, M) => Z)(implicit p0: Packer[A], p1: Packer[B], p2: Packer[C], p3: Packer[D], p4: Packer[E], p5: Packer[F], p6: Packer[G], p7: Packer[H], p8: Packer[I], p9: Packer[J], p10: Packer[K], p11: Packer[L], p12: Packer[M]): Packer[Z] =
    new ProductPacker[Z](p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12)(a => f(a(0).asInstanceOf[A], a(1).asInstanceOf[B], a(2).asInstanceOf[C], a(3).asInstanceOf[D], a(4).asInstanceOf[E], a(5).asInstanceOf[F], a(6).asInstanceOf[G], a(7).asInstanceOf[H], a(8).asInstanceOf[I], a(9).asInstanceOf[J], a(10).asInstanceOf[K], a(11).asInstanceOf[L], a(12).asInstanceOf[M]))

  def apply[Z <: Product, A, B, C, D, E, F, G, H, I, J, K, L, M, N](f: (A, B, C, D, E, F, G, H, I, J, K, L, M, N) => Z)(implicit p0: Packer[A], p1: Packer[B], p2: Packer[C], p3: Packer[D], p4: Packer[E], p5: Packer[F], p6: Packer[G], p7: Packer[H], p8: Packer[I], p9: Packer[J], p10: Packer[K], p11: Packer[L], p12: Packer[M], p13: Packer[N]): Packer[Z] =
    new ProductPacker[Z](p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13)(a => f(a(0).asInstanceOf[A], a(1).asInstanceOf[B], a(2).asInstanceOf[C], a(3).asInstanceOf[D], a(4).asInstanceOf[E], a(5).asInstanceOf[F], a(6).asInstanceOf[G], a(7).asInstanceOf[H], a(8).asInstanceOf[I], a(9).asInstanceOf[J], a(10).asInstanceOf[K], a(11).asInstanceOf[L], a(12).asInstanceOf[M], a(13).asInstanceOf[N]))

  def apply[Z <: Product, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O](f: (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O) => Z)(implicit p0: Packer[A], p1: Packer[B], p2: Packer[C], p3: Packer[D], p4: Packer[E], p5: Packer[F], p6: Packer[G], p7: Packer[H], p8: Packer[I], p9: Packer[J], p10: Packer[K], p11: Packer[L], p12: Packer[M], p13: Packer[N], p14: Packer[O]): Packer[Z] =
    new ProductPacker[Z](p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14)(a => f(a(0).asInstanceOf[A], a(1).asInstanceOf[B], a(2).asInstanceOf[C], a(3).asInstanceOf[D], a(4).asInstanceOf[E], a(5).asInstanceOf[F], a(6).asInstanceOf[G], a(7).asInstanceOf[H], a(8).asInstanceOf[I], a(9).asInstanceOf[J], a(10).asInstanceOf[K], a(11).asInstanceOf[L], a(12).asInstanceOf[M], a(13).asInstanceOf[N], a(14).asInstanceOf[O]))

  def apply[Z <: Product, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P](f: (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P) => Z)(implicit p0: Packer[A], p1: Packer[B], p2: Packer[C], p3: Packer[D], p4: Packer[E], p5: Packer[F], p6: Packer[G], p7: Packer[H], p8: Packer[I], p9: Packer[J], p10: Packer[K], p11: Packer[L], p12: Packer[M], p13: Packer[N], p14: Packer[O], p15: Packer[P]): Packer[Z] =
    new ProductPacker[Z](p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15)(a => f(a(0).asInstanceOf[A], a(1).asInstanceOf[B], a(2).asInstanceOf[C], a(3).asInstanceOf[D], a(4).asInstanceOf[E], a(5).asInstanceOf[F], a(6).asInstanceOf[G], a(7).asInstanceOf[H], a(8).asInstanceOf[I], a(9).asInstanceOf[J], a(10).asInstanceOf[K], a(11).asInstanceOf[L], a(12).asInstanceOf[M], a(13).asInstanceOf[N], a(14).asInstanceOf[O], a(15).asInstanceOf[P]))

  def apply[Z <: Product, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q](f: (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q) => Z)(implicit p0: Packer[A], p1: Packer[B], p2: Packer[C], p3: Packer[D], p4: Packer[E], p5: Packer[F], p6: Packer[G], p7: Packer[H], p8: Packer[I], p9: Packer[J], p10: Packer[K], p11: Packer[L], p12: Packer[M], p13: Packer[N], p14: Packer[O], p15: Packer[P], p16: Packer[Q]): Packer[Z] =
    new ProductPacker[Z](p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16)(a => f(a(0).asInstanceOf[A], a(1).asInstanceOf[B], a(2).asInstanceOf[C], a(3).asInstanceOf[D], a(4).asInstanceOf[E], a(5).asInstanceOf[F], a(6).asInstanceOf[G], a(7).asInstanceOf[H], a(8).asInstanceOf[I], a(9).asInstanceOf[J], a(10).asInstanceOf[K], a(11).asInstanceOf[L], a(12).asInstanceOf[M], a(13).asInstanceOf[N], a(14).asInstanceOf[O], a(15).asInstanceOf[P], a(16).asInstanceOf[Q]))

  def apply[Z <: Product, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R](f: (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R) => Z)(implicit p0: Packer[A], p1: Packer[B], p2: Packer[C], p3: Packer[D], p4: Packer[E], p5: Packer[F], p6: Packer[G], p7: Packer[H], p8: Packer[I], p9: Packer[J], p10: Packer[K], p11: Packer[L], p12: Packer[M], p13: Packer[N], p14: Packer[O], p15: Packer[P], p16: Packer[Q], p17: Packer[R]): Packer[Z] =
    new ProductPacker[Z](p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17)(a => f(a(0).asInstanceOf[A], a(1).asInstanceOf[B], a(2).asInstanceOf[C], a(3).asInstanceOf[D], a(4).asInstanceOf[E], a(5).asInstanceOf[F], a(6).asInstanceOf[G], a(7).asInstanceOf[H], a(8).asInstanceOf[I], a(9).asInstanceOf[J], a(10).asInstanceOf[K], a(11).asInstanceOf[L], a(12).asInstanceOf[M], a(13).asInstanceOf[N], a(14).asInstanceOf[O], a(15).asInstanceOf[P], a(16).asInstanceOf[Q], a(17).asInstanceOf[R]))

  def apply[Z <: Product, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S](f: (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S) => Z)(implicit p0: Packer[A], p1: Packer[B], p2: Packer[C], p3: Packer[D], p4: Packer[E], p5: Packer[F], p6: Packer[G], p7: Packer[H], p8: Packer[I], p9: Packer[J], p10: Packer[K], p11: Packer[L], p12: Packer[M], p13: Packer[N], p14: Packer[O], p15: Packer[P], p16: Packer[Q], p17: Packer[R], p18: Packer[S]): Packer[Z] =
    new ProductPacker[Z](p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18)(a => f(a(0).asInstanceOf[A], a(1).asInstanceOf[B], a(2).asInstanceOf[C], a(3).asInstanceOf[D], a(4).asInstanceOf[E], a(5).asInstanceOf[F], a(6).asInstanceOf[G], a(7).asInstanceOf[H], a(8).asInstanceOf[I], a(9).asInstanceOf[J], a(10).asInstanceOf[K], a(11).asInstanceOf[L], a(12).asInstanceOf[M], a(13).asInstanceOf[N], a(14).asInstanceOf[O], a(15).asInstanceOf[P], a(16).asInstanceOf[Q], a(17).asInstanceOf[R], a(18).asInstanceOf[S]))

  def apply[Z <: Product, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T](f: (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T) => Z)(implicit p0: Packer[A], p1: Packer[B], p2: Packer[C], p3: Packer[D], p4: Packer[E], p5: Packer[F], p6: Packer[G], p7: Packer[H], p8: Packer[I], p9: Packer[J], p10: Packer[K], p11: Packer[L], p12: Packer[M], p13: Packer[N], p14: Packer[O], p15: Packer[P], p16: Packer[Q], p17: Packer[R], p18: Packer[S], p19: Packer[T]): Packer[Z] =
    new ProductPacker[Z](p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18, p19)(a => f(a(0).asInstanceOf[A], a(1).asInstanceOf[B], a(2).asInstanceOf[C], a(3).asInstanceOf[D], a(4).asInstanceOf[E], a(5).asInstanceOf[F], a(6).asInstanceOf[G], a(7).asInstanceOf[H], a(8).asInstanceOf[I], a(9).asInstanceOf[J], a(10).asInstanceOf[K], a(11).asInstanceOf[L], a(12).asInstanceOf[M], a(13).asInstanceOf[N], a(14).asInstanceOf[O], a(15).asInstanceOf[P], a(16).asInstanceOf[Q], a(17).asInstanceOf[R], a(18).asInstanceOf[S], a(19).asInstanceOf[T]))

  def apply[Z <: Product, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U](f: (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U) => Z)(implicit p0: Packer[A], p1: Packer[B], p2: Packer[C], p3: Packer[D], p4: Packer[E], p5: Packer[F], p6: Packer[G], p7: Packer[H], p8: Packer[I], p9: Packer[J], p10: Packer[K], p11: Packer[L], p12: Packer[M], p13: Packer[N], p14: Packer[O], p15: Packer[P], p16: Packer[Q], p17: Packer[R], p18: Packer[S], p19: Packer[T], p20: Packer[U]): Packer[Z] =
    new ProductPacker[Z](p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18, p19, p20)(a => f(a(0).asInstanceOf[A], a(1).asInstanceOf[B], a(2).asInstanceOf[C], a(3).asInstanceOf[D], a(4).asInstanceOf[E], a(5).asInstanceOf[F], a(6).asInstanceOf[G], a(7).asInstanceOf[H], a(8).asInstanceOf[I], a(9).asInstanceOf[J], a(10).asInstanceOf[K], a(11).asInstanceOf[L], a(12).asInstanceOf[M], a(13).asInstanceOf[N], a(14).asInstanceOf[O], a(15).asInstanceOf[P], a(16).asInstanceOf[Q], a(17).asInstanceOf[R], a(18).asInstanceOf[S], a(19).asInstanceOf[T], a(20).asInstanceOf[U]))

  def apply[Z <: Product, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V](f: (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V) => Z)(implicit p0: Packer[A], p1: Packer[B], p2: Packer[C], p3: Packer[D], p4: Packer[E], p5: Packer[F], p6: Packer[G], p7: Packer[H], p8: Packer[I], p9: Packer[J], p10: Packer[K], p11: Packer[L], p12: Packer[M], p13: Packer[N], p14: Packer[O], p15: Packer[P], p16: Packer[Q], p17: Packer[R], p18: Packer[S], p19: Packer[T], p20: Packer[U], p21: Packer[V]): Packer[Z] =
    new ProductPacker[Z](p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18, p19, p20, p21)(a => f(a(0).asInstanceOf[A], a(1).asInstanceOf[B], a(2).asInstanceOf[C], a(3).asInstanceOf[D], a(4).asInstanceOf[E], a(5).asInstanceOf[F], a(6).asInstanceOf[G], a(7).asInstanceOf[H], a(8).asInstanceOf[I], a(9).asInstanceOf[J], a(10).asInstanceOf[K], a(11).asInstanceOf[L], a(12).asInstanceOf[M], a(13).asInstanceOf[N], a(14).asInstanceOf[O], a(15).asInstanceOf[P], a(16).asInstanceOf[Q], a(17).asInstanceOf[R], a(18).asInstanceOf[S], a(19).asInstanceOf[T], a(20).asInstanceOf[U], a(21).asInstanceOf[V]))

}
