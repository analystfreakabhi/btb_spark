import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._ // for implicit conversations
//import org.apache.spark.sql._

object SQL01 {
  // register case class external to main
  case class Employee(EmployeeID : Int, 
    LastName : String, FirstName : String, Title : String,
    BirthDate : String, HireDate : String,
    City : String, State : String, Zip : String, Country : String,
    ReportsTo : String)
    //
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local","Chapter 7")
    println(s"Running Spark Version ${sc.version}")
    //
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    //import sqlContext.createSchemaRDD // to implicitly convert an RDD to a SchemaRDD.
    import sqlContext.implicits._
    //import sqlContext._
    //import sqlContext.createDataFrame
    //import sqlContext.createExternalTable
    //
    val employeeFile = sc.textFile("/Volumes/sdxc-01/fdps-vii/data/NW-Employees-NoHdr.csv")
    println("Employee File has %d Lines.".format(employeeFile.count()))
    val employees = employeeFile.map(_.split(",")).
      map(e => Employee( e(0).trim.toInt,
        e(1), e(2), e(3), 
        e(4), e(5), 
        e(6), e(7), e(8), e(9), e(10)))
     println(employees.count)
     employees.toDF().registerTempTable("Employees")
     var result = sqlContext.sql("SELECT * from Employees")
     result.foreach(println)
     result = sqlContext.sql("SELECT * from Employees WHERE State = 'WA'")
     result.foreach(println)
     System.out.println("** Done **")
  }
}