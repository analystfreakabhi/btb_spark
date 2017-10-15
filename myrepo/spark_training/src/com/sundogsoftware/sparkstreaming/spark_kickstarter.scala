package com.sundogsoftware.sparkstreaming

import scala.math.random
import org.apache.spark.sql.SparkSession


object spark_kickstarter  {
def main(args: Array[String]) {
    //val conf = new SparkConf().setAppName("scala spark")
    //val sc1 = new SparkContext(conf)
  
   val sparkSession = SparkSession.builder.
      master("local")
      .appName("spark session example")
      .getOrCreate()
      
 print("*************************")
 print("********* HELLO WORLD *************")
  }
}

