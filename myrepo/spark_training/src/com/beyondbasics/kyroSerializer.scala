package com.beyondbasics

import java.io.ByteArrayOutputStream

import com.esotericsoftware.kryo.io.Input
import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.ClassTag

object kyroSerializer {
  def main(args: Array[String]){
    print("********** Kyro Serializer **************")
    
    if(args.length < 1 ){
      println("Please provide the output path")
      return
    }
    
    val outputPath = args(0)
    
    val conf = new SparkConf().setMaster("local").setAppName("Kyro Example")
    conf.set("spark.serializer", "org.apache.spark.serializer.KyroSerializer")
    
    val sc =  new SparkContext(conf)
    
    
  }
  
  /*
   * Used to write as Object file using kryo serialization
   */
  def saveAsObjectFile[T: ClassTag](rdd: RDD[T], path: String) {
    val kryoSerializer = new KryoSerializer(rdd.context.getConf)

    rdd.mapPartitions(iter => iter.grouped(10)
      .map(_.toArray))
      .map(splitArray => {
      //initializes kyro and calls your registrator class
      val kryo = kryoSerializer.newKryo()

      //convert data to bytes
      val bao = new ByteArrayOutputStream()
      val output = kryoSerializer.newKryoOutput()
      output.setOutputStream(bao)
      kryo.writeClassAndObject(output, splitArray)
      output.close()

      // We are ignoring key field of sequence file
      val byteWritable = new BytesWritable(bao.toByteArray)
      (NullWritable.get(), byteWritable)
    }).saveAsSequenceFile(path)
  }
}


