import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg,mean}

object DS02 {
  	//
	def getCurrentDirectory = new java.io.File( "." ).getCanonicalPath
	//
	def main(args: Array[String]) {
		println(getCurrentDirectory)
		val spark = SparkSession.builder
      .master("local")
      .appName("Chapter 9")
      .config("spark.logConf","true")
      .config("spark.logLevel","ERROR")
      .getOrCreate()
		println(s"Running Spark Version ${spark.version}")
		//
		val startTime = System.nanoTime()
		//
		// Read Data
		//
		val filePath = "/Users/ksankar/fdps-v3/"
		val cars = spark.read.option("header","true").
		  option("inferSchema","true").
	    csv(filePath + "data/car-data/car-milage.csv")
    println("Cars has "+cars.count()+" rows")
    cars.show(5)
    cars.printSchema()
    //
    cars.describe("mpg","hp","weight","automatic").show()
    //
    cars.groupBy("automatic").avg("mpg","torque","hp","weight").show()
    //
   cars.groupBy().avg("mpg","torque").show()
   cars.agg( avg(cars("mpg")), mean(cars("torque")) ).show() 
   //
    //
    val elapsedTime = (System.nanoTime() - startTime) / 1e9
    println("Elapsed time: %.2f seconds".format(elapsedTime))
    //
    println("*** That's All Folks ! ***")
    //
    spark.stop()
    //
	}
}