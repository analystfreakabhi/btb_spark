### Spark Tips and tricks

Below are some of the tips for the spark which helps for coders :

1. go to spark-shell and type sc.[\t]

   and will list all the classes/methods available for sc. object

2. scala> sc.getConf.toDebugString
   res0: String =
   spark.app.id=local-1507946461982
   spark.app.name=Spark shell
   spark.driver.host=192.168.56.1
   spark.driver.port=58213
   spark.executor.id=driver
   spark.home=c:\spark\bin\..
   spark.jars=
   spark.master=local[*]
   spark.repl.class.outputDir=C:\Users\Swagger\AppData\Local\Temp\spark-2fd8e876-7a98-494b-9eea-763a6324fe18\repl-692a121d-ba91-4bfb-8025-36b4fcbd9fb5
   spark.repl.class.uri=spark://192.168.56.1:58213/classes
   spark.sql.catalogImplementation=hive
   spark.submit.deployMode=client

3. scala> sc.getExecutorMemoryStatus
   res1: scala.collection.Map[String,(Long, Long)] = Map(192.168.56.1:58254 -> (384093388,384093388))

4. scala> spark.version //sc.version
   res4: String = 2.2.0

### **sparkSQL**

```scala
val spark = org.apache.spark.sql.SparkSession.builder
        .master("local")
        .appName("Spark CSV Reader")
        .getOrCreate;
        
        val df = spark.read
         .format("csv")
         .option("header", "true") //reading the headers
         .option("mode", "DROPMALFORMED")
         .load("hdfs:///csv/file/dir/file.csv")
         
        val df = spark.sql("SELECT * FROM csv.`csv/file/path/in/hdfs`")  
```

**Scala classes SQL**

Each of these classes is rich with a lot of functions. The diagram shows only the most
common ones we need in this chapter. You should refer to either https://spark.apache.org/docs/2.1.1/api/scala/index.html#org.apache.spark.sql.SQLContext

or to get full coverage http://spark.apache.org/docs/2.1.0/api/python/pyspark.sql.html

