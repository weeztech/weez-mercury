package com.weez.mercury.product

import java.util.concurrent.atomic.{AtomicLong, AtomicInteger}

import com.weez.mercury
import com.weez.mercury.common.{Cursor, ModelObject}
import com.weez.mercury.imports._

import scala.collection.mutable
import scala.concurrent.Future
import scala.util.{Random, Failure, Success}
import scala.util.control.NonFatal

/**
 * Created by gaojingxin on 15/2/27.
 */
object Test {

  import mercury._
  import mercury.common._
  import scala.concurrent._
  import scala.concurrent.ExecutionContext.Implicits._

  def main(args: Array[String]): Unit = {
    val app = App.start(args)
    val remoteCallManager = app.remoteCallManager
    val sessionManager = app.sessionManager
    val peer = sessionManager.ensurePeer(None)
    def call(api: String, args: (String, Any)*): Future[InstantResponse] = {
      try {
        remoteCallManager.postRequest(peer, "com.weez.mercury." + api, ModelObject(args: _*))
      } catch {
        case NonFatal(ex) => Future.failed(ex)
      }
    }
    val future = call("product.SessionService.init").flatMap {
      case ModelResponse(m) =>
        val sid: String = m.sid
        def call2(api: String): Future[InstantResponse] = {
          call(api, "sid" -> sid)
        }
        testInit(call2)
    }
    future.onComplete { result =>
      app.close()
      import scala.util._
      result match {
        case Success(ModelResponse(x)) => println(JsonModel.to(x).prettyPrint)
        case Failure(ex) => ex.printStackTrace()
        case _ => throw new IllegalStateException()
      }
    }
  }

  def testInit(call: String => Future[InstantResponse]) = {
    call("product.TestService.init").flatMap {
      case ModelResponse(_) =>
        testAccount(call)
    }
  }

  def testBy(call: String => Future[InstantResponse]) = {
    val p = Promise[ModelResponse]()
    var tryTimes = 10000
    val allStart = System.nanoTime()
    def doCall(): Unit = {
      call("product.TestService.buyProducts").onComplete {
        case Success(ModelResponse(ns)) =>
          if (tryTimes > 0) {
            tryTimes -= 1
            doCall()
          } else {
            p.success(ModelResponse(ModelObject(
              "all" -> (System.nanoTime() - allStart) / 1000000
            )))
          }
        case Failure(ex) => p.failure(ex)
      }
    }
    doCall()
    p.future
  }

  def testAccount(call: String => Future[InstantResponse]) = {
    val tryTimes = new AtomicInteger(1000)
    val allStart = System.nanoTime()
    val partials = new AtomicInteger(3)
    val p = Promise[ModelResponse]()
    def doCall(): Unit = {
      call("product.TestService.showProductAccount").onComplete {
        case Success(ModelResponse(ns)) =>
          val tt = tryTimes.decrementAndGet()
          if (tt > 0) {
            doCall()
          } else if (tt > -1000 && partials.decrementAndGet() == 0) {
            p.success(ModelResponse(ModelObject(
              "all" -> (System.nanoTime() - allStart) / 1000000,
              "acc" -> ns.account
            )))
          }
        case Failure(ex) =>
          if (tryTimes.getAndAdd(-10000000) >= 0 && partials.getAndAdd(-1000) > 0) {
            p.failure(ex)
          }
      }
    }

    for (x <- 1 to partials.get()) {
      doCall()
    }
    p.future
  }
}

object TestService extends RemoteService {
  lazy val productIDs = new Array[Long](10000)
  lazy val providerIDs = new Array[Long](100)
  lazy val warehouseIDs = new Array[Long](5)
  lazy val customerIDs = new Array[Long](1000)

  def initCustomers(c: PersistCallContext) = {
    import c._
    val start = System.currentTimeMillis
    val pc = CustomerCollection
    var i = customerIDs.length - 1
    while (i >= 0) {
      val code = "customer-" + i
      val exist = pc.byCode(code)
      if (exist.isEmpty) {
        val p = Customer(code, code, "Customer")
        pc.insert(p)
        customerIDs(i) = p.id
      } else {
        customerIDs(i) = exist.get.id
      }
      i -= 1
    }
    val cost = System.currentTimeMillis - start
    println(s"init ${customerIDs.length} customer cost $cost ms")
  }

  def initProducts(c: PersistCallContext) = {
    import c._
    val pc = ProductCollection
    var i = productIDs.length - 1
    val start = System.currentTimeMillis
    while (i >= 0) {
      val code = "product-" + i
      val exist = pc.byCode(code)
      if (exist.isEmpty) {
        val p = Product(code, code, "Product", randomPrice(), randomPrice())
        pc.insert(p)
        productIDs(i) = p.id
      } else {
        productIDs(i) = exist.get.id
      }
      i -= 1
    }
    val cost = System.currentTimeMillis - start
    println(s"init ${productIDs.length} products cost $cost ms")
  }

  def initProviders(c: PersistCallContext) = {
    import c._
    val pc = ProviderCollection
    var i = providerIDs.length - 1
    val start = System.currentTimeMillis
    while (i >= 0) {
      val code = "provider-" + i
      val exist = pc.byCode(code)
      if (exist.isEmpty) {
        val p = Provider(code, code, "Provider")
        pc.insert(p)
        providerIDs(i) = p.id
      } else {
        providerIDs(i) = exist.get.id
      }
      i -= 1
    }
    val cost = System.currentTimeMillis - start
    println(s"init ${providerIDs.length} providers cost $cost ms")
  }

  def initWarehouses(c: PersistCallContext) = {
    import c._
    val pc = WarehouseCollection
    var i = warehouseIDs.length - 1
    val start = System.currentTimeMillis
    while (i >= 0) {
      val code = "warehouse-" + i
      val exist = pc.byCode(code)
      if (exist.isEmpty) {
        val p = Warehouse(code, code, "Warehouse")
        pc.insert(p)
        warehouseIDs(i) = p.id
      } else {
        warehouseIDs(i) = exist.get.id
      }
      i -= 1
    }
    val cost = System.currentTimeMillis - start
    println(s"init ${warehouseIDs.length} warehouse cost $cost ms")
  }


  private var datetime: DateTime = _

  val init: PersistCall = { c =>
    import c._
    initWarehouses(c)
    initCustomers(c)
    initProviders(c)
    initProducts(c)
    datetime = MaxProductFlowDate.get(true).map(_.value.dt).getOrElse {
      new DateTime
    }
    complete(model())
  }

  val r = new Random()

  def randomProvider() = RefSome(providerIDs(r.nextInt(providerIDs.length)))

  def randomProduct() = RefSome(productIDs(r.nextInt(productIDs.length)))

  def randomWarehouse() = RefSome(warehouseIDs(r.nextInt(warehouseIDs.length)))

  def randomSN() = {
    val sn = r.nextInt()
    if (sn < 0) {
      (-sn).toString
    } else {
      sn.toString
    }
  }

  def randomPrice() = (r.nextInt(100) + 10) * 100

  def randomQty() = r.nextInt(20) + 1

  def randomItems[T](f: => T): Seq[T] = {
    items(r.nextInt(10) + 1)(f)
  }

  def items[T](count: Int)(f: => T): Seq[T] = {
    val rb = new mutable.ArraySeq[T](count)
    var i = 0
    while (i < count) {
      rb(i) = f
      i += 1
    }
    rb
  }

  def nextDt(): DateTime = {
    datetime = datetime.plusSeconds(10)
    datetime
  }

  val depends = Seq(StockAccount, ProductAccount, SingleProductAccount, SingleProductInventoryByStock)

  val buyProducts: PersistCall = { c =>
    import c._
    val dt = nextDt()
    val nb = "purchase-" + dt.toString
    val pc = PurchaseOrder(
      datetime = dt,
      number = nb,
      provider = randomProvider(),
      invoiceNumber = nb,
      remark = nb,
      items = randomItems {
        val qty = randomQty()
        val price = randomPrice()
        PurchaseOrderItem(
          stock = randomWarehouse(),
          product = randomProduct(),
          quantity = qty,
          totalValue = price * qty,
          remark = "product",
          singleProducts = items(qty) {
            SingleProductInfo(randomSN(), RefEmpty, RefEmpty)
          }
        )
      }
    )
    PurchaseOrderCollection.insert(pc)
    complete(model())
  }
  val showProductAccount: QueryCall = { c =>
    import c._
    try{
    val invent = mutable.Map.empty[(Ref[Entity],Ref[Product]), ProductQV]
    for (x <- 1 to 20) {
      val product = randomProduct()
      val stock = randomWarehouse()
      invent.getOrElseUpdate((stock,product), {
        StockAccount.inventory(stock,product, datetime)
      })
    }
    var account = List.empty[ModelObject]
    for (((stock,product), qv) <- invent) {
      account ::= ModelObject("wh" -> stock().asInstanceOf[Warehouse].title,
        "product" -> product().title, "qty" -> qv.quantity)
    }
    complete(model("account" -> account))
    }catch{
      case e=> e.printStackTrace()
    }
  }
}