package com.weez.mercury.product

import com.weez.mercury.common.DB.driver.simple._

class Products(tag: Tag) extends Table[(Long, String, String, String, Array[Byte], Int)](tag, "biz_products") {
  def id = column[Long]("id", O.PrimaryKey)

  def code = column[String]("code")

  def name = column[String]("name")

  def description = column[String]("description")

  def path = column[Array[Byte]]("path")

  def category = column[Int]("category")

  def * = (id, code, name, description, path, category)
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


