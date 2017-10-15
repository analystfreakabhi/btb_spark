import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.clustering.KMeans

object ML03v2 {
  //
	def getCurrentDirectory = new java.io.File( "." ).getCanonicalPath
	//
  def main(args: Array[String]): Unit = {
    println(getCurrentDirectory)
		val spark = SparkSession.builder
      .master("local")
      .appName("Chapter 11")
      .config("spark.logConf","true")
      .config("spark.logLevel","ERROR")
      .getOrCreate()
		println(s"Running Spark Version ${spark.version}")
		//
		val startTime = System.nanoTime()
		//
		val filePath = "/Users/ksankar/fdps-v3/"
		val data = spark.read.option("header","true").
		  option("inferSchema","true").
	    csv(filePath + "data/cluster-points-v2.csv")
    println("Data has "+data.count()+" rows")
    data.show(5)
    data.printSchema()
    //
    val assembler = new VectorAssembler()
    assembler.setInputCols(Array("X","Y"))
    assembler.setOutputCol("features")
    val data1 = assembler.transform(data)
    data1.show(5)
    //
    // Create the Kmeans model
    //
    var algKMeans = new KMeans().setK(2)
    var mdlKMeans = algKMeans.fit(data1)
    // Evaluate clustering by computing Within Set Sum of Squared Errors.
    var WSSSE = mdlKMeans.computeCost(data1)
    println(s"Within Set Sum of Squared Errors (K=2) = %.3f".format(WSSSE))
    // Shows the result.
    println("Cluster Centers (K=2) : " + mdlKMeans.clusterCenters.mkString("<", ",", ">"))
    println("Cluster Sizes (K=2) : " +  mdlKMeans.summary.clusterSizes.mkString("<", ",", ">"))
    //
    var predictions = mdlKMeans.transform(data1)
    predictions.show(3)
    //
    predictions.write.mode("overwrite").option("header","true").csv(filePath + "data/cluster-2K.csv")
    //
    //
    // Now let us try 4 centers
    //
    algKMeans = new KMeans().setK(4)
    mdlKMeans = algKMeans.fit(data1)
    // Evaluate clustering by computing Within Set Sum of Squared Errors.
    WSSSE = mdlKMeans.computeCost(data1)
    println(s"Within Set Sum of Squared Errors (K=4) = %.3f".format(WSSSE))
    // Shows the result.
    println("Cluster Centers (K=4) : " + mdlKMeans.clusterCenters.mkString("<", ",", ">"))
    println("Cluster Sizes (K=4) : " +  mdlKMeans.summary.clusterSizes.mkString("<", ",", ">"))
    //
    predictions = mdlKMeans.transform(data1)
    predictions.show(3)
    //
    predictions.write.mode("overwrite").option("header","true").csv(filePath + "data/cluster-4K.csv")
    //
    val elapsedTime = (System.nanoTime() - startTime) / 1e9
    println("Elapsed time: %.2f seconds".format(elapsedTime))
    //
    println("*** That's All Folks ! ***")
    //
  }
}