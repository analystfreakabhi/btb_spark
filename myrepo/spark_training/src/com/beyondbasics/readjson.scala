package com.beyondbasics

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions


object readjson {
  def main(args: Array[String]){
        print(" ************* Spark SQL - Read JSON example *************** ")

    val spark = org.apache.spark.sql.SparkSession.builder.appName("readJson").master("local").getOrCreate()
    val df = spark.sqlContext.read.format("json").load("C:\\Users\\Swagger\\Documents\\GitHub\\btb_spark\\data\\employee.json")
    df.show()  
    df.printSchema()
    df.select("name","age").show()
    df.groupBy("age").count().explain()
    //df.groupBy("age").count().show()
    //df.filter($"age" > 21).show()
    
    //df.createOrReplaceTempView("people")
    //val sqlDF = spark.sql("select * from people where age > 21")
    //sqlDF.show()
    
    // Global temporary view is tied to a system preserved database `global_temp`
    print(" ************* Global Temporary table example *************** ")
    df.createOrReplaceGlobalTempView("people_temp") 
    spark.sql("select * from global_temp.people_temp").show()
    spark.newSession().sql("select * from global_temp.people_temp").show()
    
  }
}