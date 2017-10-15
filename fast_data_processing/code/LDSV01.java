import java.util.Arrays;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;

public class LDSV01 {

  public static void main(String[] args) {
    // TODO Auto-generated method stub
    SparkConf conf = new SparkConf().setAppName("Chapter 05").setMaster("local");
    JavaSparkContext ctx = new JavaSparkContext(conf);
    JavaRDD<Integer> dataRDD = ctx.parallelize(Arrays.asList(1,2,4));
    System.out.println(dataRDD.count());
    System.out.println(dataRDD.take(3));
  }
}
