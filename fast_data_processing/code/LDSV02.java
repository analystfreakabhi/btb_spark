import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.SparkFiles;;

public class LDSV02 {

  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("Chapter 05").setMaster("local");
    JavaSparkContext ctx = new JavaSparkContext(conf);
    System.out.println("Running Spark Version : " +ctx.version());
    ctx.addFile("/Volumes/sdxc-01/fdps-vii/data/spam.data");
    JavaRDD<String> lines = ctx.textFile(SparkFiles.get("spam.data"));
    System.out.println(lines.first());
  }
}
