//
// sparkSQL.scala
//
// Code for pre 2.0 way of doing things
// Will work in 2.0, but the methods are deprecated
//
import org.apache.spark.SparkContext

// register case class external to main
case class Order(OrderID : String, CustomerID : String, EmployeeID : String,
OrderDate : String, ShipCountry : String)
//
case class OrderDetails(OrderID : String, ProductID : String, UnitPrice : Float,
Qty : Int, Discount : Float)
//
val filePath = "/Users/ksankar/fdps-v3/"
println(s"Running Spark Version ${sc.version}")
//
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext._
import sqlContext.implicits._
//
val ordersFile = sc.textFile(filePath + "data/NW-Orders-NoHdr.csv")
println("Orders File has %d Lines.".format(ordersFile.count()))
val orders = ordersFile.map(_.split(",")).
map(e => Order( e(0), e(1), e(2),e(3), e(4) ))
println(orders.count)
orders.toDF().registerTempTable("Orders")
var count = sqlContext.sql("SELECT COUNT(*) from Orders")
count.show()
var result = sqlContext.sql("SELECT * from Orders")
// result.take(10).foreach(println) - no need for ugly println. show() does a good job !
result.show(10)
result.head(3)
//
val orderDetFile = sc.textFile(filePath + "data/NW-Order-Details-NoHdr.csv")
println("Order Details File has %d Lines.".format(orderDetFile.count()))
val orderDetails = orderDetFile.map(_.split(",")).
map(e => OrderDetails( e(0), e(1), e(2).trim.toFloat,e(3).trim.toInt, e(4).trim.toFloat ))
println(orderDetails.count)
orderDetails.toDF().registerTempTable("OrderDetails")
result = sqlContext.sql("SELECT * from OrderDetails")
//result.take(10).foreach(println)
result.show(10)
result.head(3)
//
// Now the interesting part
//
result = sqlContext.sql("SELECT OrderDetails.OrderID,ShipCountry,UnitPrice,Qty,Discount FROM Orders INNER JOIN OrderDetails ON Orders.OrderID = OrderDetails.OrderID")
//No need to elaborate println(). SHow() does a good job!
//result.take(10).foreach(println)
//result.take(10).foreach(e=>println("%s | %15s | %5.2f | %d | %5.2f |".format(e(0),e(1),e(2),e(3),e(4))))
result.show(10)
result.head(3)
//
// Sales By Country
//
result = sqlContext.sql("SELECT ShipCountry, SUM(OrderDetails.UnitPrice * Qty * Discount) AS ProductSales FROM Orders INNER JOIN OrderDetails ON Orders.OrderID = OrderDetails.OrderID GROUP BY ShipCountry")
result.count()
//result.take(10).foreach(println)
//result.take(30).foreach(e=>println("%15s | %9.2f |".format(e(0),e(1))))
result.show(10)
result.head(3)
//
println("** Done **");