package com.weez.mercury.common

trait TTLBased[K] {
  def id: K

  val createTimestamp = System.nanoTime()
  private[common] var inUse = 0
  private[common] var activeTimestamp = createTimestamp
}

class TTLMap[K, V <: TTLBased[K]](timeout: Long) {

  import scala.collection.mutable

  val values = mutable.Map[K, V]()

  def clean() = {
    val now = System.nanoTime()
    val removes = mutable.ArrayBuffer[K]()
    values foreach { tp =>
      val value = tp._2
      if (value.inUse == 0 && now - value.activeTimestamp > timeout) {
        removes.append(value.id)
      }
    }
    removes map (values.remove(_).get)
  }

  def putAndLock(value: V) = {
    values.put(value.id, value)
    value.inUse += 1
  }

  def lock(id: K) = {
    values.get(id).map { value =>
      value.inUse += 1
      value
    }
  }

  def unlock(value: V) = {
    value.inUse -= 1
    if (value.inUse == 0)
      value.activeTimestamp = System.nanoTime()
  }

  def unlock(id: K): Unit = values.get(id) map unlock

  def unlockAll(ids: Seq[K]): Unit = {
    if (ids.nonEmpty)
      ids foreach (values.get(_) map unlock)
  }
}
