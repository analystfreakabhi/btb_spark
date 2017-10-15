import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{Vector,Vectors}
import org.apache.spark.mllib.clustering.KMeans

object MLlib03 {
  def parsePoints(inpLine : String) : Vector = {
    val values = inpLine.split(',')
    val x = values(0).toInt
    val y = values(1).toInt
    return Vectors.dense(x,y)
  }
  //

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local","Chapter 8")
    println(s"Running Spark Version ${sc.version}")
    //
    val dataFile = sc.textFile("/Volumes/sdxc-01/fdps-vii/data/cluster-points.csv")
    val points = dataFile.map(_.trim).filter( _.length > 1).map(line => parsePoints(line))
    //
    println(points.count())
    //
    var numClusters = 2
    val numIterations = 20
    var mdlKMeans = KMeans.train(points, numClusters, numIterations)
    //
    println(mdlKMeans.clusterCenters)
    //
    var clusterPred = points.map(x=>mdlKMeans.predict(x))
    var clusterMap = points.zip(clusterPred)
    //
    clusterMap.foreach(println)
    //
    clusterMap.saveAsTextFile("/Users/ksankar/fdps-vii/data/3xx-cluster.csv")
    //
    // Now let us try 4 centers
    //
    numClusters = 4
    mdlKMeans = KMeans.train(points, numClusters, numIterations)
    clusterPred = points.map(x=>mdlKMeans.predict(x))
    clusterMap = points.zip(clusterPred)
    clusterMap.saveAsTextFile("/Users/ksankar/fdps-vii/data/5xx-cluster.csv")
    clusterMap.foreach(println)
  }
}