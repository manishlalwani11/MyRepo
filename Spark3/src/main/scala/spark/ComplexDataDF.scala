package spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ComplexDataDF {
  
  def main(args: Array[String]){
    
    val conf = new SparkConf().setAppName("Complex Data DF")setMaster("local[*]")
    
    val sc = new SparkContext(conf)
    
    val sparkSession = new SparkSession.Builder().getOrCreate()
    

    
    val imageDF = sparkSession.read.format("json").option("multiline", true).load("file:///F:/Study/Sai_Spark_training/practice_data/complexjson/image.json")
    
    imageDF.show(false)
    imageDF.printSchema()
    
       /*   root
 |-- id: string (nullable = true)
 |-- image: struct (nullable = true)
 |    |-- height: long (nullable = true)
 |    |-- url: string (nullable = true)
 |    |-- width: long (nullable = true)
 |-- name: string (nullable = true)
 |-- thumbnail: struct (nullable = true)
 |    |-- height: long (nullable = true)
 |    |-- url: string (nullable = true)
 |    |-- width: long (nullable = true)
 |-- type: string (nullable = true)
    
    */
    
    println("============== test ==============")
    
    // This will flatten the nested struct cloumn and this will add extra columns
    val checkNestedColDF = imageDF.select("*", "thumbnail.*")
    checkNestedColDF.show(false)
    
     
    
    // Faltten the data
    
    val flattened_imageDF = imageDF.selectExpr("id", 
        "image.height as image_height", 
        "image.url as image_url",
        "image.width as image_width",
        "name",
        "type",
        "thumbnail.height as thumbnail_height",
        "thumbnail.url as thumbnail_url",
        "thumbnail.width as thumbnail_width"
    )
    
    flattened_imageDF.show()
    flattened_imageDF.printSchema()
    
    
  /*  root
 |-- id: string (nullable = true)
 |-- image_height: long (nullable = true)
 |-- image_url: string (nullable = true)
 |-- image_width: long (nullable = true)
 |-- name: string (nullable = true)
 |-- type: string (nullable = true)
 |-- thumbnail_height: long (nullable = true)
 |-- thumbnail_url: string (nullable = true)
 |-- thumbnail_width: long (nullable = true)
*/
    
    
    // here we can modify or massage the data as it is flattened  ---> LIKE CHANGING image_width to 50
    
    val modified_flattened_imageDF = flattened_imageDF.withColumn("image_width", lit("50"))
    modified_flattened_imageDF.show()
    modified_flattened_imageDF.printSchema()
    
    // Now stiching back to the old original comlplex schema
    
    val stiched_df = modified_flattened_imageDF.select(
    		col("id"),

    		struct(
    				col("image_height").alias("height"),
    				col("image_url").alias("url"),
    				col("image_width").alias("width")
    				).alias("image"),

    		col("name"),

    		struct(
    				col("thumbnail_height").alias("height"),
    				col("thumbnail_url").alias("url"),
    				col("thumbnail_width").alias("width")
    				).alias("thumbnail"),

    		col("type")

    		)

    stiched_df.show(false)
    stiched_df.printSchema()

            /*   root
 |-- id: string (nullable = true)
 |-- image: struct (nullable = true)
 |    |-- height: long (nullable = true)
 |    |-- url: string (nullable = true)
 |    |-- width: long (nullable = true)
 |-- name: string (nullable = true)
 |-- thumbnail: struct (nullable = true)
 |    |-- height: long (nullable = true)
 |    |-- url: string (nullable = true)
 |    |-- width: long (nullable = true)
 |-- type: string (nullable = true)
    
    */
    
    
  }
  
}