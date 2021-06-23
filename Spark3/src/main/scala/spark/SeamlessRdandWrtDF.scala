package spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object SeamlessRdandWrtDF {
  
  def main(args: Array[String]){
    
    val conf = new SparkConf().setAppName("Seamless read write DF").setMaster("local[*]")
    
    val sc = new SparkContext(conf)
    
    val sparkSession = new SparkSession.Builder().getOrCreate()
    import sparkSession.implicits._
    
    
    // Reading csv data
    
    val txnsDF = sparkSession.read.format("csv").load("file:///F:/Study/Sai_Spark_training/practice_data/txns")
    txnsDF.show()
    
			val structTypeTxns = StructType(StructField("txnno",StringType, true) ::
				StructField("txndate",StringType, true)::
					StructField("custno",StringType, true)::
						StructField("amount",StringType, true)::
							StructField("category",StringType, true)::
								StructField("product",StringType, true)::
									StructField("city",StringType, true)::
										StructField("state",StringType, true):: 
											StructField("spendBy",StringType, true) ::Nil)
  
  
		// 	 Reading csv data with Schema - StructType
					
		val txnsDFwithSchema = sparkSession.read.schema(structTypeTxns).format("csv").load("file:///F:/Study/Sai_Spark_training/practice_data/txns")
		txnsDFwithSchema.show()
		
		// Reading JSON data
		
		val devicesJsonDF = sparkSession.read.format("json").load("file:///F:/Study/Sai_Spark_training/practice_data/jsondata/devices.json")
    devicesJsonDF.show()
    
		//Reading parquet file
    val parquetDF = sparkSession.read.format("parquet").load("file:///F:/Study/Sai_Spark_training/practice_data/parquetdata/part-00000-7c8eb979-89d5-4130-99dd-1f9370e42562-c000.snappy.parquet")
    parquetDF.show()
    
  }
  
}