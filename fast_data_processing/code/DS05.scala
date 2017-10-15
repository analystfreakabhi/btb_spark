import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{log,log10,log1p,sqrt,hypot}


object DS05 {
  //
  def getCurrentDirectory = new java.io.File( "." ).getCanonicalPath
  //
  //  0 pclass,1 survived,2 l.name,3.f.name, 4 sex,5 age,6 sibsp,7 parch,8 ticket,9 fare,10 cabin,
  // 11 embarked,12 boat,13 body,14 home.dest
  //
  //
  def main(args: Array[String]): Unit = {
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
		var aList : List[Int] = List(10,100,1000)
		var aRDD = spark.sparkContext.parallelize(aList)
		val sqlContext = spark.sqlContext
    import sqlContext.implicits._
    var ds = spark.createDataset(aRDD)
    ds.show()
    //
    ds.select( ds("value"), log(ds("value")).as("ln")).show()
    ds.select( ds("value"), log10(ds("value")).as("log10")).show()
    ds.select( ds("value"), sqrt(ds("value")).as("sqrt")).show()
    //
		aList = List(0,10,100,1000)
		aRDD = spark.sparkContext.parallelize(aList)
    ds = spark.createDataset(aRDD)
    ds.select( ds("value"), log(ds("value")).as("ln")).show()
    ds.select( ds("value"), log1p(ds("value")).as("ln1p")).show()
    
		val filePath = "/Users/ksankar/fdps-v3/"
		val data = spark.read.option("header","true").
		  option("inferSchema","true").
	    csv(filePath + "data/hypot.csv")
    println("Data has "+data.count()+" rows")
    data.show(5)
    data.printSchema()
    //
    data.select( data("X"),data("Y"),hypot(data("X"),data("Y")).as("hypot") ).show()
    //
    val elapsedTime = (System.nanoTime() - startTime) / 1e9
    println("Elapsed time: %.2f seconds".format(elapsedTime))
    //
    println("*** That's All Folks ! ***")
    //
  }
}