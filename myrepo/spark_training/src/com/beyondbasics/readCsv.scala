package com.beyondbasics

import org.apache.spark._
import org.apache.spark.sql.SparkSession
//import spark.implicits._


object readCsv {
  def main(args: Array[String]){
    print("********* Read CSV  ************")
   val spark = org.apache.spark.sql.SparkSession.builder
        .master("local")
        .appName("Spark CSV Reader")
        .getOrCreate;
    val log = spark.read
              .format("csv")
              .option("header", "true") //reading the headers("C:\\Users\\Swagger\\Documents\\GitHub\\btb_spark\\data\\SampleCSVFile_11kb.csv")
              .load("C:\\Users\\Swagger\\Documents\\GitHub\\btb_spark\\data\\SampleCSVFile_11kb.csv")
    log.show()

  }
}