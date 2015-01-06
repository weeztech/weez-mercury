package com.weez.mercury.product

import com.weez.mercury.common._

object DataService extends RemoteService {
  def availableAssistants: QueryCall = c => {
    import c._
    val items = sql"""SELECT a.id,s.name,a.price,a.description
             FROM biz_assistant a JOIN biz_staffs s on s.id = a.id
         """.as[ModelObject].list
    completeWith("items" -> items)
  }

  def availableRooms: QueryCall = c => {
    import c._
    val items = sql"SELECT id,title,price,description FROM biz_rooms".as[ModelObject].list
    completeWith("items" -> items)
  }

  def availableDevices: QueryCall = c => {
    import c._
    def getProducts(keywords: String, start: Int, count: Int) = {
      sql"""SELECT id,code,title,price,description FROM biz_product_models
           where title like CONCAT('%',$keywords,'%') LIMIT $start,$count""".as[ModelObject]
    }
    completeWith("items" -> getProducts(request.keywords, request.start, request.count).list)
  }
}