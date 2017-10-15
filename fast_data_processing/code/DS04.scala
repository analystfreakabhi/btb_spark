import org.apache.spark.sql.SparkSession

object DS04 {
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
		val filePath = "/Users/ksankar/fdps-v3/"
		val passengers = spark.read.option("header","true").
		  option("inferSchema","true").
	    csv(filePath + "data/titanic3_02.csv")
    println("Passengers has "+passengers.count()+" rows")
    //passengers.show(5)
    //passengers.printSchema()
    //
    val passengers1 = passengers.select(passengers("Pclass"),passengers("Survived"),passengers("Gender"),passengers("Age"),passengers("SibSp"),passengers("Parch"),passengers("Fare"))
    passengers1.show(5)
    passengers1.printSchema()
    //
    passengers1.groupBy("Gender").count().show()
    passengers1.stat.crosstab("Survived","Gender").show()
    //
    passengers1.stat.crosstab("Survived","SibSp").show()
    //
    // passengers1.stat.crosstab("Survived","Age").show()
    val ageDist =  passengers1.select(passengers1("Survived"), (passengers1("age") - passengers1("age") % 10).cast("int").as("AgeBracket"))
    ageDist.show(3)
    ageDist.stat.crosstab("Survived","AgeBracket").show()    
    //    
    val elapsedTime = (System.nanoTime() - startTime) / 1e9
    println("Elapsed time: %.2f seconds".format(elapsedTime))
    //
    println("*** That's All Folks ! ***")
    //
  }
}