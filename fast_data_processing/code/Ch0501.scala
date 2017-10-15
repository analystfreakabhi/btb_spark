import org.apache.spark.SparkContext

object Ch0501 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local","Chapter 7")
    println(s"Running Spark Version ${sc.version}")
    //
    val inFile = sc.textFile("/Users/ksankar//fdps-v3/data/Line_of_numbers.csv")
    var stringsRDD = inFile.map(line => line.split(','))
    stringsRDD.take(10)
    val numbersRDD = stringsRDD.map(x => x.map(_.toDouble))
    numbersRDD.take(3)
  }
}