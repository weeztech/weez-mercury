package com.weez.mercury.product

import com.weez.mercury.imports._
import com.github.nscala_time.time.Imports._

/**
 * 供应商
 */
object ProviderService extends MasterDataService[Provider] {
  override def dataCollection = ProviderCollection
  val queryByID: QueryCall = super.queryByID
}

@packable
case class Provider(code: String,
                    title: String, description: String) extends MasterData

object ProviderCollection extends MasterDataCollection[Provider] {
  override def name: String = "provider"
}