package com.beyondbasics

import org.apache.spark._
import org.apache.spark.sql._

object readCsv_sparksql {
  def main(args: Array[String]){
        print("********* Read CSV - SparkSQL  ************")
        val spark = org.apache.spark.sql.SparkSession.builder
        .appName("sparkSQL")
        .master("local")
        .getOrCreate()
        
         val df = spark.sql("SELECT * FROM csv.`C:\\Users\\Swagger\\Documents\\GitHub\\btb_spark\\data\\SampleCSVFile_11kb.csv`")
         df.show()
  }
}