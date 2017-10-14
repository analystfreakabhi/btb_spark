package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec
import org.apache.spark.mllib.recommendation._

object MovieRecommendationsALS {
  
  /** Load up a Map of movie IDs to movie names. */
  def loadMovieNames() : Map[Int, String] = {
    
    // Handle character encoding issues:
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    // Create a Map of Ints to Strings, and populate it from u.item.
    var movieNames:Map[Int, String] = Map()
    
     val lines = Source.fromFile("../ml-100k/u.item").getLines()
     for (line <- lines) {
       var fields = line.split('|')
       if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
       }
     }
    
     return movieNames
  }
  
  /** Our main function where the action happens */
  def main(args: Array[String]) {
    
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "MovieRecommendationsALS")
    
    println("Loading movie names...")
    val nameDict = loadMovieNames()
 
    val data = sc.textFile("../ml-100k/u.data")
    
    val ratings = data.map( x => x.split('\t') ).map( x => Rating(x(0).toInt, x(1).toInt, x(2).toDouble) ).cache()
    
    // Build the recommendation model using Alternating Least Squares
    println("\nTraining recommendation model...")
    
    val rank = 8
    val numIterations = 20
    
    val model = ALS.train(ratings, rank, numIterations)
    
    val userID = args(0).toInt
    
    println("\nRatings for user ID " + userID + ":")

    val userRatings = ratings.filter(x => x.user == userID)
    
    val myRatings = userRatings.collect()
    
    for (rating <- myRatings) {
      println(nameDict(rating.product.toInt) + ": " + rating.rating.toString)
    }
    
    println("\nTop 10 recommendations:")
    
    val recommendations = model.recommendProducts(userID, 10)
    for (recommendation <- recommendations) {
      println( nameDict(recommendation.product.toInt) + " score " + recommendation.rating )
    }

  }
}