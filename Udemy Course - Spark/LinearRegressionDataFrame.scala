package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._

import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.types._
import org.apache.spark.ml.linalg.Vectors

object LinearRegressionDataFrame {
  
  
  /** Our main function where the action happens */
  def main(args: Array[String]) {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("LinearRegressionDF")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()
      
    // Load up our page speed / amount spent data in the format required by MLLib
    // (which is label, vector of features)
      
    // In machine learning lingo, "label" is just the value you're trying to predict, and
    // "feature" is the data you are given to make a prediction with. So in this example
    // the "labels" are the first column of our data, and "features" are the second column.
    // You can have more than one "feature" which is why a vector is required.
    val inputLines = spark.sparkContext.textFile("../regression.txt")
    val data = inputLines.map(_.split(",")).map(x => (x(0).toDouble, Vectors.dense(x(1).toDouble)))
    
    // Convert this RDD to a DataFrame
    import spark.implicits._
    val colNames = Seq("label", "features")
    val df = data.toDF(colNames: _*)
    
    // Note, there are lots of cases where you can avoid going from an RDD to a DataFrame.
    // Perhaps you're importing data from a real database. Or you are using structured streaming
    // to get your data.
     
    // Let's split our data into training data and testing data
    val trainTest = df.randomSplit(Array(0.5, 0.5))
    val trainingDF = trainTest(0)
    val testDF = trainTest(1)
    
    // Now create our linear regression model
    val lir = new LinearRegression()
      .setRegParam(0.3) // regularization 
      .setElasticNetParam(0.8) // elastic net mixing
      .setMaxIter(100) // max iterations
      .setTol(1E-6) // convergence tolerance
    
    // Train the model using our training data
    val model = lir.fit(trainingDF)
    
    // Now see if we can predict values in our test data.
    // Generate predictions using our linear regression model for all features in our 
    // test dataframe:
    val fullPredictions = model.transform(testDF).cache()
    
    // This basically adds a "prediction" column to our testDF dataframe.
    
    // Extract the predictions and the "known" correct labels.
    val predictionAndLabel = fullPredictions.select("prediction", "label").rdd.map(x => (x.getDouble(0), x.getDouble(1)))
    
    // Print out the predicted and actual values for each point
    for (prediction <- predictionAndLabel) {
      println(prediction)
    }
    
    // Stop the session
    spark.stop()

  }
}