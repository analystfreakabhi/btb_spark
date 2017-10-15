import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{split,pow,isnan}
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.log4j.{Level, Logger}

object ML04v2 extends Serializable {
  //
	def getCurrentDirectory = new java.io.File( "." ).getCanonicalPath
  //
	def parseRating(row:Row) : Rating = {
	  val aList = row.getList[String](0)
	  Rating(aList.get(0).toInt,aList.get(1).toInt,aList.get(2).toDouble) //.getInt(0), row.getInt(1), row.getDouble(2))
	}
	//
	def rowSqDiff(row:Row) : Double = {
	  math.pow( (row.getDouble(2) - row.getFloat(3).toDouble),2)
	}
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
		// To turn off INFO messages
		//
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)		// INFO, TRACE,...
		val startTime = System.nanoTime()
		//
		val filePath = "/Users/ksankar/fdps-v3/"
		val movies = spark.read.text(filePath + "data/medium/movies.dat")
		movies.show(5,truncate=false)
    movies.printSchema()
		val ratings = spark.read.text(filePath + "data/medium/ratings.dat")
		ratings.show(5,truncate=false)
		val users = spark.read.text(filePath + "data/medium/users.dat")
		users.show(5,truncate=false)
		//
    println("Got %d ratings from %d users on %d movies.".format(ratings.count(), users.count(), movies.count()))
    //
    // Transformation
    // This is a kludge. Let me know if there is a better way
    //
    val ratings1 = ratings.select(split(ratings("value"),"::")).as("values")
    ratings1.show(5)
    val ratings2 = ratings1.rdd.map(row => parseRating(row))
    ratings2.take(3).foreach(println)
    //
    val ratings3 = spark.createDataFrame(ratings2)
    ratings3.show(5)
    //
    // split data
    //
    val Array(train, test) = ratings3.randomSplit(Array(0.8, 0.2))
    println("Train = "+train.count()+" Test = "+test.count())
    //
    // create algorithm object
    //
    val algALS = new ALS()
    algALS.setItemCol("product") // Otherwise will get exception "Field "item" does not exist"
    algALS.setRank(12)
    algALS.setRegParam(0.1) // was regularization parameter, was lambda in MLlib
    algALS.setMaxIter(20)
    val mdlReco = algALS.fit(train)
     //
		// Now let us use the model to predict our test set
		//
    val predictions = mdlReco.transform(test)
    predictions.show(5)
    predictions.printSchema()
    //
    // some of the recommendation is NaN. 
    // Running into https://issues.apache.org/jira/browse/SPARK-14489 = cold Start
    // So filter them out before calculating MSE et al
    // Test Code to find the NaN. Not used
    /*
    val nanState = predictions.na.fill(99999.0)
    println(nanState.filter(nanState("prediction") > 99998).count())
    nanState.filter(nanState("prediction") > 99998).show(5)
    * 
    */
    //
    val pred = predictions.na.drop()
    println("Orig = "+predictions.count()+" Final = "+ pred.count() + " Dropped = "+ (predictions.count() - pred.count()))
    // Calculate RMSE & MSE
    val evaluator = new RegressionEvaluator()
		evaluator.setLabelCol("rating")
		var rmse = evaluator.evaluate(pred)
		println("Root Mean Squared Error = "+"%.3f".format(rmse))
		//
		evaluator.setMetricName("mse")
		var mse = evaluator.evaluate(pred)
		println("Mean Squared Error = "+"%.3f".format(mse))
		mse = pred.rdd.map(r => rowSqDiff(r)).reduce(_+_) / predictions.count().toDouble
		println("Mean Squared Error (Calculated) = "+"%.3f".format(mse))
		//
    //
    val elapsedTime = (System.nanoTime() - startTime) / 1e9
    println("Elapsed time: %.2f seconds".format(elapsedTime))
    //
    println("*** That's All Folks ! ***")
    // MLlib
    // 1.3.0 (2/18/15)
    // *** Model Performance Metrics ***
    // MSE = 0.87495
    // RMSE = 0.93539
    // 1.4.0 RC2 (5/24/15)
    // *** Model Performance Metrics ***
    // MSE = 0.87185
    // RMSE = 0.93373
    // 1.6.2 (6/22/16)
    // MSE = 0.87423
    // RMSE = 0.93500
    // ML
    // 2.0.0 (7/23/16)
    // Root Mean Squared Error = 0.871
    // Mean Squared Error = 0.759
    //
  }
}