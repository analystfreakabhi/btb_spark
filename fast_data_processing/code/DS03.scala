import org.apache.spark.sql.SparkSession

object DS03 {
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
    // cars.show(5)
    // cars.printSchema()
    //
    val cor = cars.stat.corr("hp","weight")
    println("hp to weight : Correlation = %.4f".format(cor))
    val cov = cars.stat.cov("hp","weight")
		println("hp to weight : Covariance = %.4f".format(cov))
		//
		cars.stat.crosstab("automatic","NoOfSpeed").show()
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