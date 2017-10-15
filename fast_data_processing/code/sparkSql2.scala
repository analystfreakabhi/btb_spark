//
// sparkSQL2.scala
//
// Code for Spark 2.0 way of doing things
// 
// register case class external to main
case class Employee(EmployeeID : String, 
   LastName : String, FirstName : String, Title : String,
   BirthDate : String, HireDate : String,
   City : String, State : String, Zip : String,  Country : String,
   ReportsTo : String)
//
case class Order(OrderID : String, CustomerID : String, EmployeeID : String,
  OrderDate : String, ShipCountry : String)
//
case class OrderDetails(OrderID : String, ProductID : String, UnitPrice : Double,
  Qty : Int, Discount : Double)
//
val filePath = "/Users/ksankar/fdps-v3/"
println(s"Running Spark Version ${sc.version}")
//
val employees = spark.read.option("header","true").
	csv(filePath + "data/NW-Employees.csv").as[Employee]
println("Employees has "+employees.count()+" rows")
employees.show(5)
employees.head()
//
employees.createOrReplaceTempView("EmployeesTable")
var result = spark.sql("SELECT * from EmployeesTable")
result.show(5)
result.head(3)
//
employees.explain(true)
//
result = spark.sql("SELECT * from EmployeesTable WHERE State = 'WA'")
result.show(5)
result.head(3)
//
result.explain(true)
//
// Handling multiple tables with Spark SQL
//
val orders = spark.read.option("header","true").
	csv(filePath + "data/NW-Orders.csv").as[Order]
println("Orders has "+orders.count()+" rows")
orders.show(5)
orders.head()
orders.dtypes
//
val orders = spark.read.option("header","true").
	option("inferSchema","true").
	csv(filePath + "data/NW-Orders.csv").as[Order]
println("Orders has "+orders.count()+" rows")
orders.show(5)
orders.head()
orders.dtypes // verify column types
//
val orderDetails = spark.read.option("header","true").
	option("inferSchema","true").
	csv(filePath + "data/NW-Order-Details.csv").as[OrderDetails]
println("Order Details has "+orderDetails.count()+" rows")
orderDetails.show(5)
orderDetails.head()
orderDetails.dtypes // verify column types
//
//orders.createTempView("OrdersTable")
orders.createOrReplaceTempView("OrdersTable")
result = spark.sql("SELECT * from OrdersTable")
result.show(10)
result.head(3)
//
orderDetails.createOrReplaceTempView("OrderDetailsTable")
var result = spark.sql("SELECT * from OrderDetailsTable")
result.show(10)
result.head(3)
//
// Now the interesting part
//
result = spark.sql("SELECT OrderDetailsTable.OrderID,ShipCountry,UnitPrice,Qty,Discount FROM OrdersTable INNER JOIN OrderDetailsTable ON OrdersTable.OrderID = OrderDetailsTable.OrderID")
result.show(10)
result.head(3)
//
// Sales By Country
//
result = spark.sql("SELECT ShipCountry, SUM(OrderDetailsTable.UnitPrice * Qty * Discount) AS ProductSales FROM OrdersTable INNER JOIN OrderDetailsTable ON OrdersTable.OrderID = OrderDetailsTable.OrderID GROUP BY ShipCountry")
result.count()
result.show(10)
result.head(3)
result.orderBy($"ProductSales".desc).show(10) // Top 10 by Sales
//
println("** Done **");