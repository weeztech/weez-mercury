package com.weez.mercury.product

import com.weez.mercury.common._
import com.github.nscala_time.time.Imports._

/**
 * 主数据基接口
 */
trait MasterData extends Entity {
  val code: String
  val title: String
  val description: String
}

object MasterDataHelper {

  import DTOHelper._

  implicit final class MasterDataHelper[E <: MasterData](private val self: Ref[E]) extends AnyVal {
    @inline def codeTitleAsMO(implicit db: DBSessionQueryable): ModelObject = {
      self.asMO { (mo, o) =>
        mo.code = o.code
        mo.title = o.title
      }
    }
  }

}