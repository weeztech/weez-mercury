package com.weez.mercury.product

import com.weez.mercury.common.DB.driver.simple._

class ProductModels(tag: Tag) extends Table[(Long, String, String, Double, String)](tag, "biz_product_models") {
  def id = column[Long]("id", O.PrimaryKey)

  def code = column[String]("code")

  def title = column[String]("title")

  def price = column[Double]("price")

  def description = column[String]("description")

  def * = (id, code, title, price, description)
}

object ProductModels extends TableQuery(new ProductModels(_))

class Products(tag: Tag) extends Table[(Long, String, String, String, Long)](tag, "biz_products") {
  def id = column[Long]("id", O.PrimaryKey)

  def code = column[String]("code")

  def name = column[String]("name")

  def description = column[String]("description")

  def modelId = column[Long]("model_id")

  def model = foreignKey("model_fk", modelId, ProductModels)(_.id)

  def * = (id, code, name, description, modelId)
}

object Products extends TableQuery(new Products(_))

class ProductPrices(tag: Tag) extends Table[(Long, Long, Long, BigDecimal)](tag, "biz_product_prices") {
  def productId = column[Long]("product_id")

  def startTime = column[Long]("start_time")

  def endTime = column[Long]("end_time")

  def price = column[BigDecimal]("price")

  def * = (productId, startTime, endTime, price)

  def pk = primaryKey("pk", (productId, startTime))
}

object ProductPrices extends TableQuery(new ProductPrices(_))


