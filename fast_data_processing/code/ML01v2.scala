import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.corr
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.evaluation.RegressionEvaluator

object ML01v2 {
	//
	def getCurrentDirectory = new java.io.File( "." ).getCanonicalPath
	//
	def main(args: Array[String]) {
		println(getCurrentDirectory)
		val spark = SparkSession.builder
      .master("local")
      .appName("Chapter 11")
      .config("spark.logConf","true")
      .config("spark.logLevel","ERROR")
      .getOrCreate()
		println(s"Running Spark Version ${spark.version}")
		//
		val filePath = "/Users/ksankar/fdps-v3/"
		val cars = spark.read.option("header","true").
		  option("inferSchema","true").
	    csv(filePath + "data/car-data/car-milage.csv")
    println("Cars has "+cars.count()+" rows")
    cars.show(5)
    cars.printSchema()
		//
		// Let us find summary statistics
		//
    cars.describe("mpg","hp","weight","automatic").show()
		//
		// correlations
		//
		var cor = cars.stat.corr("hp","weight")
		println("hp to weight : Correlation = %2.4f".format(cor))
		var cov = cars.stat.cov("hp","weight")
		println("hp to weight : Covariance = %2.4f".format(cov))
		//
	  cor = cars.stat.corr("RARatio","width")
		println("Rear Axle Ratio to width : Correlation = %2.4f".format(cor))
		cov = cars.stat.cov("RARatio","width")
		println("Rear Axle Ratio to width : Covariance = %2.4f".format(cov))
		//
		// Linear Regression
		//
		// Transformation to a labeled data that Linear Regression Can use 
		val cars1 = cars.na.drop() 
		val assembler = new VectorAssembler()
    assembler.setInputCols(Array("displacement","hp","torque","CRatio","RARatio","CarbBarrells","NoOfSpeed","length","width","weight","automatic"))
    assembler.setOutputCol("features")
    val cars2 = assembler.transform(cars1)
    cars2.show(40)
    //
    // Split into training & test
    //
    val train = cars2.filter(cars1("weight") <= 4000)
    val test = cars2.filter(cars1("weight") > 4000)
    test.show()
    println("Train = "+train.count()+" Test = "+test.count())
		val algLR = new LinearRegression()
		algLR.setMaxIter(100)
		algLR.setRegParam(0.3)
		algLR.setElasticNetParam(0.8)
		algLR.setLabelCol("mpg")
		//
		val mdlLR = algLR.fit(train)
		//
		println(s"Coefficients: ${mdlLR.coefficients} Intercept: ${mdlLR.intercept}")
		val trSummary = mdlLR.summary
    println(s"numIterations: ${trSummary.totalIterations}")
    println(s"Iteration Summary History: ${trSummary.objectiveHistory.toList}")
    trSummary.residuals.show()
    println(s"RMSE: ${trSummary.rootMeanSquaredError}")
    println(s"r2: ${trSummary.r2}")
    //
		// Now let us use the model to predict our test set
		//
    val predictions = mdlLR.transform(test)
    predictions.show()
    // Calculate RMSE & MSE
    val evaluator = new RegressionEvaluator()
		evaluator.setLabelCol("mpg")
		val rmse = evaluator.evaluate(predictions)
		println("Root Mean Squared Error = "+"%6.3f".format(rmse))
		//
		evaluator.setMetricName("mse")
		val mse = evaluator.evaluate(predictions)
		println("Mean Squared Error = "+"%6.3f".format(mse))
    //
    println("** That's All Folks **");
	}
}