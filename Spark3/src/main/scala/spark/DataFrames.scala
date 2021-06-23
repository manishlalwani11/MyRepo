package spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions._

object DataFrames {

case class dummySchema(col1: String, col2: String)

case class dummySchema1(name: String, place: String, age: Integer)

case class txnsSchema(txnno: String, txndate: String, custno: String, amount: String, category: String, product: String, city: String, state: String, spendBy: String)

def main(args: Array[String]) {


	// =================================================== DATAFRAMES =============================================

	// 1. Using schemaRDD

	val conf = new SparkConf().setAppName("DataFrame practice").setMaster("local[*]")

			val sc = new SparkContext(conf)

			val sparkSession = SparkSession.builder().getOrCreate()
			import sparkSession.implicits._

			val data11 = sc.textFile("file:///F:/Study/Sai_Spark_training/practice_data/txns")

			val mapSplitSchemaRDD = data11.map(x => x.split(",")).map(x => txnsSchema(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))

			val mapSplit11DF = mapSplitSchemaRDD.toDF()

			mapSplit11DF.show()

			// 2. Using ROWStructType

			val structType = StructType(StructField("txnno",StringType, true) ::
				StructField("txndate",StringType, true)::
					StructField("custno",StringType, true)::
						StructField("amount",StringType, true)::
							StructField("category",StringType, true)::
								StructField("product",StringType, true)::
									StructField("city",StringType, true)::
										StructField("state",StringType, true):: 
											StructField("spendBy",StringType, true) ::Nil)

			val rowRDD = data11.map(x => x.split(",")).map(x => Row(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))  // ROW RDD

			val rowRDDdf = sparkSession.createDataFrame(rowRDD, structType)

			rowRDDdf.show(50)
			
			// withColumn --> Used to create a dataframe with new column
			
			// val newColDF = rowRDDdf.withColumn("New Column", expr("if((spendBy==cash)? 0 : 1)"))  How to do this via expr ???
			
			 val newColDF = rowRDDdf.withColumn("New Column", when(col("spendBy") === "cash", "0").otherwise("1"))
			
	   	 newColDF.show(50)
}


}