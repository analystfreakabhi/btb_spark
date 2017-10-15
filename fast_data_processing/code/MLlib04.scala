import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._ // for implicit conversations
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.mllib.recommendation.ALS

object MLlib04 {
  
  def parseRating1(line : String) : (Int,Int,Double,Int) = {
    val x = line.split("::")
    val userId = x(0).toInt
    val movieId = x(1).toInt
    val rating = x(2).toDouble
    val timeStamp = x(3).toInt/10
    return (userId,movieId,rating,timeStamp)
  }
  //
  def parseRating(x : (Int,Int,Double,Int)) : Rating = {
    val userId = x._1
    val movieId = x._2
    val rating = x._3
    val timeStamp = x._4 // ignore
    return new Rating(userId,movieId,rating)
  }
  //
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local","Chapter 8")
    println(s"Running Spark Version ${sc.version}")
    //
    val moviesFile = sc.textFile("/Volumes/sdxc-01/fdps-vii/data/medium/movies.dat")
    val moviesRDD = moviesFile.map(line => line.split("::"))
    println(moviesRDD.count())
    //
    val ratingsFile = sc.textFile("/Volumes/sdxc-01/fdps-vii/data/medium/ratings.dat")
    val ratingsRDD = ratingsFile.map(line => parseRating1(line))
    println(ratingsRDD.count())
    //
    ratingsRDD.take(5).foreach(println) // always check the RDD
    //
    val numRatings = ratingsRDD.count()
    val numUsers = ratingsRDD.map(r => r._1).distinct().count()
    val numMovies = ratingsRDD.map(r => r._2).distinct().count()
    println("Got %d ratings from %d users on %d movies.".
         format(numRatings, numUsers, numMovies))
    //
    // Split dataset into training, validation & test
    // We can use random. But here we will use the last digit of the time stamp
    //
    val trainSet = ratingsRDD.filter(x => (x._4 % 10) < 6).map(x=>parseRating(x))
    val validationSet = ratingsRDD.filter(x => (x._4 % 10) >= 6 & (x._4 % 10) < 8).map(x=>parseRating(x))
    val testSet = ratingsRDD.filter(x => (x._4 % 10) >= 8).map(x=>parseRating(x))
    println("Training: "+ "%d".format(trainSet.count()) + 
      ", validation: " + "%d".format(validationSet.count()) + ", test: " +
      "%d".format(testSet.count()) + ".")
    //
    // Now train the model using the training set
    val rank = 10
    val numIterations = 20
    val mdlALS = ALS.train(trainSet,rank,numIterations)
    //
    // prepare validation set for prediction
    //
    val userMovie = validationSet.map { 
      case Rating(user, movie, rate) =>(user, movie)
    }
    //
    // Predict and convert to Key-Value PairRDD
    val predictions = mdlALS.predict(userMovie).map {
      case Rating(user, movie, rate) => ((user, movie), rate)
    }
    //
    println(predictions.count())
    predictions.take(5).foreach(println)
    //
    // Now convert the validation set to PairRDD
    //
    val validationPairRDD = validationSet.map(r => ((r.user, r.product), r.rating))
    println(validationPairRDD.count())
    validationPairRDD.take(5).foreach(println)
    println(validationPairRDD.getClass())
    println(predictions.getClass())
    //
    // Now join the validation set with Predictions
    // Then we can figure out how good our recommendations are
    // Tip:
    //   Need to import org.apache.spark.SparkContext._ 
    //   Then MappedRDD would be converted implicitly to PairRDD
    //
    val ratingsAndPreds = validationPairRDD.join(predictions) 
    println(ratingsAndPreds.count())
    ratingsAndPreds.take(3).foreach(println)
    //
    val mse = ratingsAndPreds.map(r => {
      math.pow((r._2._1 - r._2._2),2)
    }).reduce(_+_) / ratingsAndPreds.count()
    val rmse = math.sqrt(mse)
    println("*** Model Performance Metrics ***")
    println("MSE = %2.5f".format(mse))
    println("RMSE = %2.5f".format(rmse))
    println("** Done **")
    // 1.3.0 (2/18/15)
    // *** Model Performance Metrics ***
    // MSE = 0.87495
    // RMSE = 0.93539
    // 1.4.0 RC2 (5/24/15)
    // *** Model Performance Metrics ***
    // MSE = 0.87185
    // RMSE = 0.93373
  }
}