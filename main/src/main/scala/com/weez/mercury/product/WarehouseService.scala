package com.weez.mercury.product

import com.weez.mercury.common._
import com.github.nscala_time.time.Imports._

class WarehouseService extends RemoteService {

}

/**
 * 仓库
 */
@packable
case class Warehouse(code: String,
                     title: String,
                     description: String) extends MasterData

object WarehouseCollection extends MasterDataCollection[Warehouse] {
  override def name: String = "warehouse"
}
