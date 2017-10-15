import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.tree.DecisionTree

object MLlib02 {
  //
  def getCurrentDirectory = new java.io.File( "." ).getCanonicalPath
  //
  //  0 pclass,1 survived,2 l.name,3.f.name, 4 sex,5 age,6 sibsp,7 parch,8 ticket,9 fare,10 cabin,
  // 11 embarked,12 boat,13 body,14 home.dest
  //
  def str2Double(x: String) : Double = {
    try {
      x.toDouble
    } catch {
      case e: Exception => 0.0
    }
  }
  //
  def parsePassengerDataToLP(inpLine : String) : LabeledPoint = {
    val values = inpLine.split(',')
    //println(values)
    //println(values.length)
    //
    val pclass = str2Double(values(0))
    val survived = str2Double(values(1))
    // skip last name, first name
    var sex = 0
    if (values(4) == "male") {
      sex = 1
    }
    var age = 0.0 // a better choice would be the average of all ages
    age = str2Double(values(5))
    //
    var sibsp = 0.0
    age = str2Double(values(6))
    //
    var parch = 0.0
    age = str2Double(values(7))
    //
    var fare = 0.0
    fare = str2Double(values(9))
    return new LabeledPoint(survived,Vectors.dense(pclass,sex,age,sibsp,parch,fare))
  }
  //
  def main(args: Array[String]): Unit = {
    println(getCurrentDirectory)
    val sc = new SparkContext("local","Chapter 8")
    println(s"Running Spark Version ${sc.version}")
    //
    val dataFile = sc.textFile("/Volumes/sdxc-01/fdps-vii/data/titanic3_01.csv")
    val titanicRDDLP = dataFile.map(_.trim).filter( _.length > 1).
      map(line => parsePassengerDataToLP(line))
    //
    println(titanicRDDLP.count())
    //titanicRDDLP.foreach(println)
    //
    println(titanicRDDLP.first().label)
    println(titanicRDDLP.first().features)
    //
    val categoricalFeaturesInfo = Map[Int, Int]()
    val mdlTree = DecisionTree.trainClassifier(titanicRDDLP, 2, // numClasses
        categoricalFeaturesInfo, // all features are continuous
        "gini", // impurity
        5, // Maxdepth
        32) //maxBins
    //
    //println(mdlTree.depth)
    println(mdlTree)
    //
    // Let us predict on the data set and see how well it works
    // In real world, we should split the data to train & test; then predict the test data
    //
    val predictions = mdlTree.predict(titanicRDDLP.map(x=>x.features))
    val labelsAndPreds = titanicRDDLP.map(x=>x.label).zip(predictions)
    //
    val mse = labelsAndPreds.map( vp => math.pow( (vp._1 - vp._2),2 ) ).
       reduce(_+_) / labelsAndPreds.count()
    println("Mean Squared Error = " + "%6f".format(mse))
    //
    // Mean Squared Error = 0.190222
    //
    // labelsAndPreds.foreach(println)
    //
    val correctVals = labelsAndPreds.aggregate(0.0)((x, rec) => x + (rec._1 == rec._2).compare(false), _ + _)
    val accuracy = correctVals/labelsAndPreds.count()
    println("Accuracy = " + "%3.2f%%".format(accuracy*100))
    // Accuracy = 80.98%
    println("*** Done ***")
  }
}