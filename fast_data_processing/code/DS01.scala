import org.apache.spark.sql.SparkSession

object DS01 {
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
	    csv(filePath + "data/spark-csv/cars.csv")
    println("Cars has "+cars.count()+" rows")
    cars.show(5)
    cars.printSchema()
    //
    // Write data
    // csv format with headers
    cars.write.mode("overwrite").option("header","true").csv(filePath + "data/cars-out-csv.csv")
    // Parquet format
    cars.write.mode("overwrite").partitionBy("year").parquet(filePath + "data/cars-out-pqt")
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