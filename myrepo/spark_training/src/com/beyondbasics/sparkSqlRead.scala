package com.beyondbasics
import org.apache.spark.sql.SparkSession


object sparkSqlRead {
  def main(args: Array[String]){
    val spark = org.apache.spark.sql.SparkSession.builder
                                .appName("SparkSQL example")
                                .master("local")
                                .config("spark.logConf","true")
                                .config("spark.logLevel","ERROR")
                                .getOrCreate()
    val versn = spark.version
    print("********* SparkSQL Examples ***************")
    println(s"Running Spark Version $versn")
    
    val filepath = "C:\\Users\\Swagger\\Documents\\GitHub\\btb_spark\\data\\"
    val startTime = System.nanoTime()
    
    val cars =  spark.read.option("header", "True").option("inferSchema", "True").csv("C:\\Users\\Swagger\\Documents\\GitHub\\btb_spark\\data\\cars.csv")
    println("Cars has "+cars.count()+ " rows")
    cars.write.mode("overwrite").option("header","true").csv(filepath + "cars-out-csv.csv")
    cars.write.mode("overwrite").partitionBy("year").parquet(filepath + "cars-out.pqt")
   
  }
}