package com.beyondbasics

import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.functions.

case class employee(age:String, id:String, name:String)

object jsonDS {
  def main(args: Array[String]){
    val spark = org.apache.spark.sql.SparkSession.builder.appName("DataSet Example with Json").master("local").getOrCreate()
    import spark.implicits._

    
    //val ds = spark.read.format("json").load("C:\\Users\\Swagger\\Documents\\GitHub\\btb_spark\\data\\employee.json").to
    val ds = spark.read.json("C:\\Users\\Swagger\\Documents\\GitHub\\btb_spark\\data\\employee.json").as[employee]
    ds.show()
  }
}