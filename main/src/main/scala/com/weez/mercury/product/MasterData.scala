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

abstract class MasterDataCollection[E <: MasterData : Packer] extends RootCollection[E] {
  override def fxFunc: Option[(E, DBSessionQueryable) => Seq[String]] = Some { (e, db) =>
    Seq(e.code, e.title, e.description)
  }
}

trait MasterDataService[E <: MasterData] extends RemoteService {
  def dataCollection: MasterDataCollection[E]

  def queryByID(c: QueryCallContext): Unit = {
    import c._
    val mo = dataCollection(request.id: Long).map { e =>
      val mo = new ModelObject(Map.empty)
      mo.id = e.id.toString
      mo.code = e.code
      mo.title = e.title
      mo
    }
    complete(mo.orNull)
  }
}