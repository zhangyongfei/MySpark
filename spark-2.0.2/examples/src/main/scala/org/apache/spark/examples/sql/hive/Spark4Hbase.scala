package org.apache.spark.examples.sql.hive

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hive.conf.HiveConf

case class Person(name:String, age:Int)

object Spark4Hbase {
   def main(args: Array[String]) {
     System.setProperty("hadoop.home.dir", "D:/home/hadoop")
     System.setProperty("HADOOP_USER_NAME", "hive")
     
    val warehouseLocation = "hdfs://sdc165.sefonsoft.com:8020/apps/hive/warehouse"

    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      .master("local[*]")
      //.config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()
      
    val path = "E:/zhangyongfei/company/SDCManager/sql/spark-2.0.2/examples/src/main/resources/core-site.xml"
      
    spark.sparkContext.hadoopConfiguration.addResource(path)
    spark.sparkContext.hadoopConfiguration.addResource("E:/zhangyongfei/company/SDCManager/sql/spark-2.0.2/examples/src/main/resources/hbase-site.xml")
    spark.sparkContext.hadoopConfiguration.set(HiveConf.ConfVars.METASTOREURIS.varname, "thrift://sdc165.sefonsoft.com:9083");
    spark.sparkContext.hadoopConfiguration.setInt(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES.varname, 3);
    spark.sparkContext.hadoopConfiguration.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
	  spark.sparkContext.hadoopConfiguration.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
	  spark.sparkContext.hadoopConfiguration.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname,
	      "false");
	  System.setProperty(HiveConf.ConfVars.PREEXECHOOKS.varname, " ");
	  System.setProperty(HiveConf.ConfVars.POSTEXECHOOKS.varname, " ");
    
    import spark.implicits._
    import spark.sql
    import org.apache.spark.sql._
    
    try{
      sql("CREATE TABLE spark_data ( " +  
         "id string,  " + 
         "name STRING,  " + 
         "gender STRING,  " + 
         "chinese STRING, " + 
         "math STRING" + 
         ")STORED BY \"org.apache.hadoop.hive.hbase.HBaseStorageHandler\"  " +  
         "WITH SERDEPROPERTIES (\"hbase.columns.mapping\" = \":key,cf1:name,cf1:gender,cf2:chinese,cf2:math\")" +  
         "TBLPROPERTIES (\"hbase.table.name\" = \"hbase_spark\")");
    } catch {
      case ex: AnalysisException => "error:" + ex.getMessage();
    }
    
     //val people = sc.textFile("D:\\SparkSql.txt").map(_.split("@@")).map( p => Person(p(0),p(1).trim.toInt)).toDF()

     //people.registerTempTable("people")
    
     sql("INSERT INTO default.spark_data " +
         //"(id, name, gender, chinese, math) " + 
         "VALUES ('002', 'Tom', 'man', '90', '91')")

     val teenagers = sql("SELECT id, name, gender, chinese, math  FROM default.spark_data")

     teenagers.map( t => " id: " + t(0) +
                         " gender: " + t(2) +
                         " chinese: " + t(3) +
                         " math: " + t(4)).collect().foreach(println)
                         
     sql("DROP TABLE default.spark_data")

     spark.stop()
   }
}