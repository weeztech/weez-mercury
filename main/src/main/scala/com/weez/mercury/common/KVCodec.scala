package com.weez.mercury.common

/**
 * Created by gaojingxin on 15/1/10.
 */
object KVCodec {
  /**
   * 布尔类型
   */
  final val FTYPE_BOOLEAN = 0;
  /**
   * 定点有符号数
   */
  final val FTYPE_NUM = 1;
  /**
   * 浮点类型
   */
  final val FTYPE_DOUBLE = 2;
  /**
   *
   */
  final val FTYPE_STRING = 3;
  final val FTYPE_DATETIME = 4;
  final val FTYPE_BYTES = 4;

  def append(buffer: Array[Byte], offset: Integer, value: Boolean): Integer = ???

  def appendDesc(buffer: Array[Byte], offset: Integer, value: Boolean): Integer = ???

  def append(buffer: Array[Byte], offset: Integer, value: Integer): Integer = ???

  def appendDesc(buffer: Array[Byte], offset: Integer, value: Integer): Integer = ???

  def append(buffer: Array[Byte], offset: Integer, value: Long): Integer = ???

  def appendDesc(buffer: Array[Byte], offset: Integer, value: Long): Integer = ???


  def append(buffer: Array[Byte], offset: Integer, value: String): Integer = ???

  def appendDesc(buffer: Array[Byte], offset: Integer, value: String): Integer = ???

  def append(buffer: Array[Byte], offset: Integer, value: Array[Byte], valueStart: Integer = 0,
             valueCount: Integer = (-1)): Integer = ???

  def appendDesc(buffer: Array[Byte], offset: Integer, value: Array[Byte], valueStart: Integer = 0,
             valueCount: Integer = (-1)): Integer = ???

}
