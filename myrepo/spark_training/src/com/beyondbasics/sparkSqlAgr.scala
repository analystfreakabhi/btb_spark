package com.beyondbasics

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg,mean}

object sparkSqlAgr {
  def main(args: Array[String]){
    val spark = org.apache.spark.sql.SparkSession.builder.appName("SparkSQL").master("local").getOrCreate()
    print("***** Spark SQL - Aggregation **********")
    
    val filepath = "C:\\Users\\Swagger\\Documents\\GitHub\\btb_spark\\data\\"
    val df = spark.read.option("header", "True").option("inferSchema", "True").csv(filepath + "car-milage.csv")
    println("cars-mileage has" + df.count() + " rows")
    df.show()
    df.printSchema()
    
    df.describe("mpg","hp","weight","automatic").show()
    df.groupBy("automatic").avg("mpg","torque").show()
        print("***** Spark SQL - avg / mean **********")

    df.agg(avg(df("mpg")), mean(df("torque"))).show()
  }
}