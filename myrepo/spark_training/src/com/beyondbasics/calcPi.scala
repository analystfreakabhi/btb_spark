package com.beyondbasics

import org.apache.spark.sql.SparkSession
import org.apache.spark._

import scala.math.random

object calcPi {
  def main(args:Array[String]){
    print("******* Calculation for the PI ************")
    val spark = SparkSession
                .builder
                .master("local")
                .appName("Spark Pi")
                .getOrCreate()
    
         val slices = if (args.length > 0 ) args(0).toInt else 2 //
         val n = math.min(100000L * slices, Int.MaxValue).toInt
         val count = spark.sparkContext.parallelize(1 until n, slices).map{i => 
           val x = random * 2 - 1
           val y = random * 2 - 1
           if ( x*x + y*y <= 1 ) 1 else 0
         }.reduce( _ + _ )
         
         print("Pi is roughly "+ 4.0 * count / (n - 1))
         spark.stop()
  }
}