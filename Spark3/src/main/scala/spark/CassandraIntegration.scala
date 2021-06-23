package spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CassandraIntegration {
  
  
   def main(args: Array[String]){
     
     val conf = new SparkConf().setAppName("casandra integration").setMaster("local[*]").set("spark.driver.allowMultipleContexts","true")
     val sc = new SparkContext(conf)
     
     sc.setLogLevel("Error")
     val spark = SparkSession.builder()
         .config("spark.cassandra.connection.host", "localhost")
         .config("spark.cassandra.connection.port", "9042")
         .getOrCreate()
     
     import spark.implicits._
     
     
     val txn_cassandra_df = spark.read.format("org.apache.spark.sql.cassandra")
                            .option("keyspace", "test11")
                            .option("table", "txnrecords")
                            .load()
                            
     txn_cassandra_df.show()
     
     
   }
  
}