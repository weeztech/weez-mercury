package com.weez.mercury.common

import com.huaban.analysis.jieba.JiebaSegmenter

object FullTextSearch {
  private val kc = new JiebaSegmenter()
  private val emptyStringSet = Set[String]()
  private val emptyStringMap = Map[String,Boolean]()

  def split2(text: String*): scala.collection.Map[String,Boolean] = {
    if (text ne null) {
      val sb = new StringBuilder()
      for (s <- text) {
        sb.append(s).append(' ')
      }
      if (sb.length > 0) {
        val words = kc.sentenceProcess(sb.toString())
        var i = words.size() - 1
        if (i >= 0) {
          val words2 = scala.collection.mutable.Map[String,Boolean]()
          do {
            words2.put(words.get(i).getToken,true)
            i -= 1
          } while (i >= 0)
          return words2
        }
      }
    }
    emptyStringMap
  }


  def split(text: String*): scala.collection.Set[String] = {
    if (text ne null) {
      val sb = new StringBuilder()
      for (s <- text) {
        sb.append(s).append(' ')
      }
      if (sb.length > 0) {
        val words = kc.sentenceProcess(sb.toString())
        var i = words.size() - 1
        if (i >= 0) {
          val words2 = scala.collection.mutable.Set[String]()
          do {
            words2.add(words.get(i).getToken)
            i -= 1
          } while (i >= 0)
          return words2
        }
      }
    }
    emptyStringSet
  }
}
