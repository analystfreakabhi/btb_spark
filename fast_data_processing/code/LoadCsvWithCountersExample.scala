import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkFiles;
import org.apache.spark.api.java.JavaSparkContext;
import au.com.bytecode.opencsv.CSVReader;
import java.io.StringReader;

object LoadCsvWithCountersExample {
	def main(args: Array[String]) {
		val sc = new SparkContext("local","Chapter 6")
		println(s"Running Spark Version ${sc.version}")
		val invalidLineCounter = sc.accumulator(0);
		val invalidNumericLineCounter = sc.accumulator(0);
		val inFile = sc.textFile("/Volumes/sdxc-01/fdps-vii/data/Line_of_numbers.csv");
		val splitLines = inFile.flatMap(line => {
			try {
				val reader = new CSVReader(new StringReader(line))
				Some(reader.readNext())
			} catch {
			case _ => {
				invalidLineCounter += 1
						None
			}
			}
		})
		val numericData = splitLines.flatMap(line => {
			try {
				Some(line.map(_.toDouble))
			} catch {
			case _ => {
				invalidNumericLineCounter += 1
						None
			}
			}
		})
		val summedData = numericData.map(row => row.sum)
		println(summedData.collect().mkString(","))
		println("Errors: "+invalidLineCounter+","
				+invalidNumericLineCounter)
	}
}