package com.weez.mercury.common

trait Packer[T] {
  def apply(value: T): Array[Byte]

  def unapply(buf: Array[Byte]): T
}