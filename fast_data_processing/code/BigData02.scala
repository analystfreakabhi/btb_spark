/*import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._ // for implicit conversations
import org.apache.spark.sql._
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.PairRDDFunctions */

object BigData02 { /*
	def main(args: Array[String]): Unit = {
			val sc = new SparkContext("local","Chapter 8")
			println(s"Running Spark Version ${sc.version}")
			//
			val conf = HBaseConfiguration.create()
			conf.set(TableInputFormat.INPUT_TABLE, "test")

			val admin = new HBaseAdmin(conf)
			println(admin.isTableAvailable("test"))

			val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
					classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
					classOf[org.apache.hadoop.hbase.client.Result])
      println(hBaseRDD.count())
      //
      hBaseRDD.foreach(println) // will print bytes
      hBaseRDD.foreach(e=> ( println("%s | %s |".format( Bytes.toString(e._1.get()),e._2) ) ) )
			//
			println("** Read Done **")
      //
      // create a pair RDD "row4":"value4"
      // save it in column family "d"
      //
      val testMap = Map("row4" -> "value4")
      val pairs = sc.parallelize(List(("row4","value4")))
      pairs.foreach(println)
      //
      //Function to convert our RDD to the required format for HBase
      //
      def convert(triple: (String, String)) = {
        val p = new Put(Bytes.toBytes(triple._1))
        p.add(Bytes.toBytes("cf"), Bytes.toBytes("d"), Bytes.toBytes(triple._2))
        (new org.apache.hadoop.hbase.io.ImmutableBytesWritable, p)
      }
      //
      val jobConfig: JobConf = new JobConf(conf, this.getClass)
      jobConfig.setOutputFormat(classOf[TableOutputFormat])
      jobConfig.set(TableOutputFormat.OUTPUT_TABLE, "test")
      //
      new PairRDDFunctions(pairs.map(convert)).saveAsHadoopDataset(jobConfig)
      //
      println("** Write Done **")
      //
      val status = admin.getClusterStatus();
      println("HBase Version : " +status.getHBaseVersion())
      println("Average Load : "+status.getAverageLoad())
      println("Backup Master Size : " + status.getBackupMastersSize())
      println("Balancer On : " + status.getBalancerOn())
      println("Cluster ID : "+ status.getClusterId())
      println("Server Info : " + status.getServerInfo())
	} */
}