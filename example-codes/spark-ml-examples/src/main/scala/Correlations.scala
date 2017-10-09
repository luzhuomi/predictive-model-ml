import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.util.MLUtils

object Correlations {
	val sc = new SparkContext("local", "shell")	
	// correlation 
	val seriesX:RDD[Double] = sc.textFile("data/basic/series1.txt").map(_.toDouble)
	val seriesY:RDD[Double] = sc.textFile("data/basic/series2.txt").map(_.toDouble)
	val correlation: Double = Statistics.corr(seriesX, seriesY, "pearson")

	val vec1:RDD[Vector] = MLUtils.loadVectors(sc, "data/basic/vector1.txt")
	val correlation2: Matrix = Statistics.corr(vec1, "pearson")
	
}
