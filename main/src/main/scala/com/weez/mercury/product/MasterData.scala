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

object MasterData {

  implicit final class MasterData2ModelObject(private val self: MasterData) extends AnyVal {
    def asSimpleMO: ModelObject = {
      import self._
      ModelObject(
        "id" -> id,
        "code" -> code,
        "title" -> title
      )
    }
  }

  implicit final class MasterDataRef2ModelObject(private val self: Ref[_ <: MasterData]) extends AnyVal {
    def asSimpleMO(implicit db: DBSessionQueryable): ModelObject = {
      if (self.isEmpty) null else self().asSimpleMO
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
      import e._
      ModelObject(
        "id" -> id,
        "code" -> code,
        "title" -> title,
        "description" -> description
      )
    }
    completeWith("result" -> mo.orNull)
  }
}