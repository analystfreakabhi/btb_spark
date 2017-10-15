import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.graphx._

object AlphaGo {
  def main(args: Array[String]) {
	println(new java.io.File( "." ).getCanonicalPath)
	/* Uncomment for running in IDE like Eclipse
	//val conf = new SparkConf(false) // skip loading external settings
	//.setMaster("local") // could be "local[4]" for 4 threads
	//.setAppName("Chapter 9")
	//.set("spark.logConf", "false") //"true")
	//val sc = new SparkContext(conf) // ("local","AlphaGo") if using directly
  //sc.setLogLevel("WARN") // ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN
  */
	println(s"Running Spark Version ${sc.version}")
	//
	val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "false") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .option("delimiter","|")
      .load("file:/Volumes/sdxc-01/Strata-2016-LON/reTweetNetwork-small.psv")
  df.show(5)
  df.count()
  //
  case class User(name:String, location:String, tz : String, fr:Int,fol:Int)
  case class Tweet(id:String,count:Int)

  val graphData = df.rdd
  graphData.take(2).foreach(println)
  
  val vert1 = graphData.map(row => (row(3).toString.toLong,User(row(4).toString,row(5).toString,row(6).toString,row(7).toString.toInt,row(8).toString.toInt)))
  vert1.count()
  vert1.take(3).foreach(println)
  
  val vert2 = graphData.map(row => (row(9).toString.toLong,User(row(10).toString,row(11).toString,row(12).toString,row(13).toString.toInt,row(14).toString.toInt)))
  vert2.count()
  vert2.take(3).foreach(println)
  
  val vertX = vert1.++(vert2)
  vertX.count()
  
  val edgX = graphData.map(row => (Edge(row(3).toString.toLong,row(9).toString.toLong,Tweet(row(0).toString,row(1).toString.toInt))))
  edgX.take(3).foreach(println)
  
  val rtGraph = Graph(vertX,edgX)
  //
  val ranks = rtGraph.pageRank(0.1).vertices
  ranks.take(2)
  val topUsers = ranks.sortBy(_._2,false).take(3).foreach(println)
  val topUsersWNames = ranks.join(rtGraph.vertices).sortBy(_._2._1,false).take(3).foreach(println)
  //
  //How big ?
  rtGraph.vertices.count
  rtGraph.edges.count
  //
  // How many retweets ?
  //
  val iDeg = rtGraph.inDegrees
  val oDeg = rtGraph.outDegrees
  //
  iDeg.take(3)
  iDeg.sortBy(_._2,false).take(3).foreach(println)
  //
  oDeg.take(3)
  oDeg.sortBy(_._2,false).take(3).foreach(println)
  //
  // max retweets
  val topRT = iDeg.join(rtGraph.vertices).sortBy(_._2._1,false).take(3).foreach(println)
  val topRT1 = oDeg.join(rtGraph.vertices).sortBy(_._2._1,false).take(3).foreach(println)
  //
  println("** That's All Folks ! **")
  println("** Done **")
  }
}