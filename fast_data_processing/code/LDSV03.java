import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.SparkFiles;;

public class LDSV03 {

  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("Chapter 05").setMaster("local");
    JavaSparkContext ctx = new JavaSparkContext(conf);
    System.out.println("Running Spark Version : " +ctx.version());
    ctx.addFile("/Volumes/sdxc-01/fdps-vii/data/Line_of_numbers.csv");
    //
    JavaRDD<String> lines = ctx.textFile(SparkFiles.get("Line_of_numbers.csv"));
    //
    JavaRDD<String[]> numbersStrRDD = lines.map(new Function<String,String[]>() {
      public String[] call(String line) {return line.split(",");}
    });
    List<String[]> val = numbersStrRDD.take(1);
    for (String[] e : val) {
      for (String s : e) {
        System.out.print(s+" ");
      }
      System.out.println();
    }
    //
    JavaRDD<String> strFlatRDD = lines.flatMap(new FlatMapFunction<String,String>() {
      public Iterator<String> call(String line) {return Arrays.asList(line.split(",")).iterator();}
    });
    List<String> val1 = strFlatRDD.collect();
    for (String s : val1) {
      System.out.print(s+" ");
      }
    System.out.println();
    //
    JavaRDD<Integer> numbersRDD = strFlatRDD.map(new Function<String,Integer>() {
      public Integer call(String s) {return Integer.parseInt(s);}
    });
    List<Integer> val2 = numbersRDD.collect();
    for (Integer s : val2) {
      System.out.print(s+" ");
      }
    System.out.println();
    //
    Integer sum = numbersRDD.reduce(new Function2<Integer,Integer,Integer>() {
      public Integer call(Integer a, Integer b) {return a+b;}
    });
    System.out.println("Sum = "+sum);
  }
}
