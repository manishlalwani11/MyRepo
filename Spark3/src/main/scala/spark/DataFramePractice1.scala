package spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object DataFramePractice1 {

	def main(args: Array[String]){

		val conf = new SparkConf().setAppName("DF practice1").setMaster("local[*]")
				val sc = new SparkContext(conf)

				val sparkSession = new SparkSession.Builder().getOrCreate()
				import sparkSession.implicits._

				// Reading CSV usdata without header

				val usDataDefault = sparkSession.read.format("csv").load("file:///F:/Study/Sai_Spark_training/practice_data/usdata.csv")
				usDataDefault.show()

				// Reading CSV usdata WITH header

				val usDataWithHeader = sparkSession.read.format("csv").option("header", true).load("file:///F:/Study/Sai_Spark_training/practice_data/usdata.csv")
				usDataWithHeader.show()
				usDataWithHeader.printSchema()

				// We can use and options("inferSchema", "true") to get the actual schema

				// Reading AVRO data using custom avro dependency

				val arvoDF = sparkSession.read.format("com.databricks.spark.avro").load("file:///F:/Study/Sai_Spark_training/practice_data/avrodata/part-00000-7dd8e6ae-a6a0-43ae-8708-d68c2dae15d2-c000.avro")
				arvoDF.show()

				// Reading usdata.csv and removing header using RDD.first() method

				val usDataRDD = sc.textFile("file:///F:/Study/Sai_Spark_training/practice_data/usdata.csv")
				val header = usDataRDD.first()
				val filteredUSdata = usDataRDD.filter(x => x!=header)
				filteredUSdata.take(10).foreach(println)

				val rowRDD = filteredUSdata.map(x => x.split(",")).map(x => Row(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10),x(11),x(12)))

				val structTypeUSdata = StructType(StructField("first_name",StringType, true) ::
					StructField("last_name",StringType, true)::
						StructField("company",StringType, true)::
							StructField("address",StringType, true)::
								StructField("city",StringType, true)::
									StructField("county",StringType, true)::
										StructField("state",StringType, true)::
											StructField("zip",StringType, true):: 
												StructField("age",StringType, true) ::
													StructField("phone1",StringType, true) :: 
														StructField("phone2",StringType, true) :: 
															StructField("email",StringType, true) :: 
																StructField("web",StringType, true) :: Nil)

				val convertedDF = sparkSession.createDataFrame(rowRDD, structTypeUSdata)
        convertedDF.show()
        
        // Reading usdata.csv and removing header using mapPartition() method  -------------- ?????????????????????
        
        // Reading XML file using custom xml dependency
        
        val xmlDF = sparkSession.read.format("com.databricks.spark.xml").option("rowTag", "book").load("file:///F:/Study/Sai_Spark_training/practice_data/complexjson/book.xml")
        xmlDF.show()
        
        //xmlDF.write.format("json").save("file:///F:/Study/Sai_Spark_training/1/")
        
        
        // from_json --> Used to convert bad data i.e. in a single coloumn of a df to a good df
        
        val badDf = sparkSession.read.format("csv").option("delimiter","~").load("file:///F:/Study/Sai_Spark_training/practice_data/jsondata/devices.json").toDF("jsonData")
        badDf.show(false)
        
        // converting to good df using from_json
        
        val structTypeDevicesJson = StructType(Array(
            StructField("device_id", StringType, true),
            StructField("device_name", StringType, true),
            StructField("humidity", StringType, true),
            StructField("lat", StringType, true),
            StructField("long", StringType, true),
            StructField("scale", StringType, true),
            StructField("temp", StringType, true),
            StructField("timestamp", StringType, true),
            StructField("zipcode", StringType, true)
            
        ))
        
        val goodDF = badDf.select(from_json(col("jsonData"), structTypeDevicesJson).alias("structdata"))
                          .select("structdata.*")
                          
           goodDF.show()
           
           
        // to_json ---> to convert good json to bad json i.e. with single coloumn  
        val badDF_new = goodDF.select(to_json(struct(col("*"))).alias("newBadjsonData"))
        badDF_new.show(false)
         
        
	}

}