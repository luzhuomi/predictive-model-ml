import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

/*
data preparation

mkdir c:\tmp\input
cp data\wordcount\TheCompleteSherlockHolmes.txt c:\tmp\input

*/

object SimpleApp {
	def main(args: Array[String]) = {

		val conf = new SparkConf().setAppName("Wordcount Application")
		val sc = new SparkContext(conf)
		val textFile = sc.textFile("c:/tmp/input/")
		val counts = textFile.flatMap(line => line.split(" "))
			.map((word:String) => (word, 1))
			.reduceByKey(_ + _)
		counts.saveAsTextFile("c:/tmp/output/")

	}
}

/*
spark-submit  --class SimpleApp target/scala-2.11/spark-examples_2.11-0.1.0.jar
*/
