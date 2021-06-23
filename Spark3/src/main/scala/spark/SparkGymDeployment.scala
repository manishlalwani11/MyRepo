package spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions



object SparkGymDeployment {


case class dummySchema(col1: String, col2: String)

case class dummySchema1(name: String, place: String, age: Integer)

case class txnsSchema(txnno: String, txndate: String, custno: String, amount: String, category: String, product: String, city: String, state: String, spendBy: String)

def main(args: Array[String]) {

	val conf = new SparkConf().setAppName("first").setMaster("local[*]")

			val sc = new SparkContext(conf)

			/*val data = sc.textFile("file:///F:/Study/Sai_Spark_training/practice_data/txns")


			val data1 = List("Telangana->Hyderabad,TamilNadu->Chennai,Andhara->Vizag,Telangana->Kammam")

			val gymData = data.filter(x => x.contains("Gymnastics"))

			val flattenedData = data1.flatMap(x => x.split(","))  //  ["Telangana->Hyderabad","TamilNadu->Chennai","Andhara->Vizag","Telangana->Kammam"]

			val splitByMap = flattenedData.map(a => a.split("->"))

			val filteredList = splitByMap.filter(x => x.apply(0).equalsIgnoreCase("Telangana"))

			val result = filteredList.map(x => x.apply(1))

			result.foreach(println)

			println

			val data2 = List("Sai,Andhra,40~Aditya,Telangana,50~Haasya,TamilNadu,20~Manju,Andhra,50~Geetha,TamilNadu,70~Ravi,Andhra,20~Satya,TamilNadu,30")

			val splittedFlatMap = data2.flatMap(x => x.split("~"))

			val splittedMap = splittedFlatMap.map(x => x.split(","))

			val schemaRdd = splittedMap.map(x => dummySchema1(x(0), x(1), Integer.valueOf(x(2).toString())))

			val res = schemaRdd.filter(x => (x.place.equalsIgnoreCase("TamilNadu") && x.age > 30))

			res.foreach(a => println(a.name))*/

			/*val datawithSchema = splitByMap.map(ab => dummySchema(ab.apply(0), ab.apply(1)))

 val filteredList = datawithSchema.filter(x => x.col1.equalsIgnoreCase("Telangana"))

 val result = filteredList.map(x => x.col2)*/



			/*val a = splitByMap.map(x => x.apply(1));

  a.take(10).foreach(println)

  val splitByMap = gymData.map(x => x.split(","))

  splitByMap.foreach(println)

  println("========== AFter FlatMap ===========")

  val splitByFlatMap = gymData.flatMap(x => x.split(","))

  splitByFlatMap.take(10).foreach(println)*/

			//gymData.saveAsTextFile("file:///C:/datawrite11")


			// Joining RDD

			val dataFile1 = sc.textFile("file:///F:/Study/Sai_Spark_training/practice_data/file1.txt.txt")
			val dataFile2 = sc.textFile("file:///F:/Study/Sai_Spark_training/practice_data/file2.txt.txt")

			dataFile1.foreach(println)

			println
			val mapSplit1 = dataFile1.map(x => x.split(","))
			val mapSplit1pairRDD = mapSplit1.map(x => (x(0).toInt, x(1)))   // Making a Pair RDD

			val mapSplit2pairRDD = dataFile2.map(x => x.split(",")).map(x => (x(0).toInt, x(1)))

			val joinedRDD = mapSplit1pairRDD.join(mapSplit2pairRDD); // join results in a Tuple ->  RDD[(Int, (String, String))]

	joinedRDD.foreach(println)

	val femaleNamesRDD = joinedRDD.filter(x => x._2._2.equalsIgnoreCase("female")).map(x => x._2._1)

	femaleNamesRDD.foreach(println)

	//converting tuple back to RDD i.e. RDD[String]

	val originalRDD = joinedRDD.map(x => x._1 + "," + x._2._1 + "," + x._2._2)
	originalRDD.foreach(println)
	
	
}



}