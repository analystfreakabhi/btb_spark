import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{to_date,month,year}

object DS06 {
  //
  def getCurrentDirectory = new java.io.File( "." ).getCanonicalPath
  //
  def main(args: Array[String]): Unit = {
    println(getCurrentDirectory)
		val spark = SparkSession.builder
      .master("local")
      .appName("Chapter 9 - Data Wrangling")
      .config("spark.logConf","true")
      .config("spark.logLevel","ERROR")
      .getOrCreate()
		println(s"Running Spark Version ${spark.version}")
		//
		//
		// To turn off INFO messages
		//
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)		// INFO, TRACE,...
		val startTime = System.nanoTime()
		//
		val filePath = "/Users/ksankar/fdps-v3/"
		val orders = spark.read.option("header","true").
		  option("inferSchema","true").
	    csv(filePath + "data/NW/NW-Orders-01.csv")
    println("Orders has "+orders.count()+" rows")
    orders.show(5)
    orders.printSchema()
    //
		val orderDetails = spark.read.option("header","true").
		  option("inferSchema","true").
	    csv(filePath + "data/NW/NW-Order-Details.csv")
    println("Order Details has "+orderDetails.count()+" rows")
    orderDetails.show(5)
    orderDetails.printSchema()
    //
    // Questions to Answer
    // 1.	How many orders were placed by each customer? 
    // 2.	How many orders were placed in each country ?
    // 3.	How many orders were placed per month ? per year ?
    // 4.	What is the Total Sales for each customer, by year ?
    // 5.	What is the average order by customer, by year ?
    //  
    // First 2 are easy - let us answer them first
    //
    val orderByCustomer = orders.groupBy("CustomerID").count()
    orderByCustomer.sort(orderByCustomer("count").desc).show(5) // We have out ans #1
    //
    val orderByCountry = orders.groupBy("ShipCountry").count()
    orderByCountry.sort(orderByCountry("count").desc).show(5) // ans #2
    //
    //# For the next set of questions, let us transform the data
    //# 1. Add OrderTotal column to the Orders DataFrame
    //# 1.1. Add Line total to order details
    //# 1.2. Aggregate total by order id
    //# 1.3. Join order details & orders to add the order total
    //# 1.4. Check if there are any null columns
    //# 2. Add a date column
    //# 3. Add month and year
    //
    //# 1.1. Add Line total to order details
    val orderDetails1 = orderDetails.select(orderDetails("OrderID"),
                                       (
                                           (orderDetails("UnitPrice") *
                                           orderDetails("Qty")) - 
                                           (
                                               (orderDetails("UnitPrice") *
                                               orderDetails("Qty")) * orderDetails("Discount")
                                           )
                                        ).as("OrderPrice"))
    orderDetails1.show(5)
    //# 1.2. Aggregate total by order id
    val orderTot = orderDetails1.groupBy("OrderID").sum("OrderPrice").alias("OrderTotal")
    orderTot.sort("OrderID").show(5)
    //# 1.3. Join order details & orders to add the order total
    val orders1 = orders.join(orderTot, orders("OrderID").equalTo(orderTot("OrderID")), "inner")
      .select(orders("OrderID"),
            orders("CustomerID"),
            orders("OrderDate"),
            orders("ShipCountry").alias("ShipCountry"),
            orderTot("sum(OrderPrice)").alias("Total"))
    //
    orders1.sort("CustomerID").show()
    //
    // # 1.4. Check if there are any null columns
    orders1.filter(orders1("Total").isNull).show()
    //
    // # 2. Add a date column
    //
    val orders2 = orders1.withColumn("Date",to_date(orders1("OrderDate")))
    orders2.show(2)
    orders2.printSchema()
    //
    // # 3. Add month and year
    val orders3 = orders2.withColumn("Month",month(orders2("OrderDate"))).withColumn("Year",year(orders2("OrderDate")))
    orders3.show(2)
    //
    // Q 3. How many orders by month/year ?
    val ordersByYM = orders3.groupBy("Year","Month").sum("Total").as("Total")
    ordersByYM.sort(ordersByYM("Year"),ordersByYM("Month")).show()
    //
    // Q 4. Total Sales for each customer by year
    var ordersByCY = orders3.groupBy("CustomerID","Year").sum("Total").as("Total")
    ordersByCY.sort(ordersByCY("CustomerID"),ordersByCY("Year")).show()
    // Q 5. Average order by customer by year
    ordersByCY = orders3.groupBy("CustomerID","Year").avg("Total").as("Total")
    ordersByCY.sort(ordersByCY("CustomerID"),ordersByCY("Year")).show()
    // Q 6. Average order by customer
    val ordersCA = orders3.groupBy("CustomerID").avg("Total").as("Total")
    ordersCA.sort(ordersCA("avg(Total)").desc).show()
    //
    val elapsedTime = (System.nanoTime() - startTime) / 1e9
    println("Elapsed time: %.2f seconds".format(elapsedTime))
    //
    println("*** That's All Folks ! ***")
    //
  }
}