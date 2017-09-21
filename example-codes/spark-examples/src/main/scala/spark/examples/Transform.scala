import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._


/*
To transform the SVM data into TSV data so that we can visualize it
input file
1 0:102 1:230
0 0:123 1:56
0 0:22  1:2
1 0:74 1:102
output files


ones/part-00000
102    230
74     102

zeros/part-00000

123    56
22     2



data preparation

hdfs dfs -mkdir /data/transform/
hdfs dfs -rm -r /output
hdfs dfs -rm -r /data/transform/input.txt
hdfs dfs -put data/transform/input.txt /data/transform/

*/

object Transform {
	// val hdfs_nn = "10.1.0.1"
	val hdfs_nn = "127.0.0.1"
	def main(args: Array[String]) = {

		val conf = new SparkConf().setAppName("ETL (Transform) Example")
		val sc   = new SparkContext(conf)
		// load the file
		val input:RDD[String] = sc.textFile(s"hdfs://${hdfs_nn}:9000/data/transform/")
		// split by spaces
		val tokenizeds:RDD[Array[String]] = input.map(line => line.split(" "))
		tokenizeds.cache()
		
		// process all the ones
		val ones = tokenizeds
		.filter(tokenized => tokenized(0) == "1")
		.map(tokenized => { 
			val x = (tokenized(1).split(":"))(1)
			val y = (tokenized(2).split(":"))(1)
			List(x,y).mkString("\t")
		})
		ones.saveAsTextFile(s"hdfs://${hdfs_nn}:9000/output/ones")

		val zeros = tokenizeds
		.filter(tokenized => tokenized(0) == "0")
		.map(tokenized => { 
			val x = (tokenized(1).split(":"))(1)
			val y = (tokenized(2).split(":"))(1)
			List(x,y).mkString("\t")
		})
		zeros.saveAsTextFile(s"hdfs://${hdfs_nn}:9000/output/zeros")
		
	}
}

/*
sbt package
/opt/spark-1.4.1-bin-hadoop2.6/bin/spark-submit --class Transform target/scala-2.10/spark-examples_2.10-0.1.0.jar 
*/