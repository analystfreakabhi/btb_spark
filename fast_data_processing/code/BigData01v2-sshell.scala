//
// Parquet save examples
// run from spark-shell
//
// register case class external to main
case class Order(OrderID : String, CustomerID : String, EmployeeID : String,
	OrderDate : String, ShipCountry : String)
//
case class OrderDetails(OrderID : String, ProductID : String, UnitPrice : Float,
	Qty : Int, Discount : Float)
//
println(s"Running Spark Version ${spark.version}")
//
val filePath = "/Users/ksankar/fdps-v3/"
val orders = spark.read.option("header","true").csv(filePath + "data/NW-Orders.csv")
println("Orders has "+orders.count()+" rows")
orders.show(3)
//
val orderDetails = spark.read.option("header","true").
option("inferSchema","true").csv(filePath + "data/NW-Order-Details.csv")
println("Order Details has "+orderDetails.count()+" rows")
orderDetails.show(3)
//
println("Saving in Parquet Format ....")
//
// Parquet Operations
//
orders.write.parquet(filePath + "Orders_Parquet")
//
// Let us read back the file
//
println("Reading back the Parquet Format ....")
val parquetOrders = spark.read.parquet(filePath + "Orders_Parquet")
println("Orders_Parquet has "+parquetOrders.count()+" rows")
parquetOrders.show(3)
//
// Save our Sales By Country Report as parquet
//
// Create views for tables
//
orders.createOrReplaceTempView("OrdersTable")
orderDetails.createOrReplaceTempView("OrderDetailsTable")
val result = spark.sql("SELECT ShipCountry, SUM(OrderDetailsTable.UnitPrice * Qty * Discount) AS ProductSales FROM OrdersTable INNER JOIN OrderDetailsTable ON OrdersTable.OrderID = OrderDetailsTable.OrderID GROUP BY ShipCountry")
result.show(3)
result.write.parquet(filePath + "SalesByCountry_Parquet")
//
println("** Done **");
