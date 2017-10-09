import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.util.MLUtils


object SummaryStats {
	val sc = new SparkContext("local", "shell")	
	val observations:RDD[Vector] = MLUtils.loadVectors(sc, "data/basic/vector1.txt") // an RDD of Vectors

	// Compute column summary statistics.
	val summary: MultivariateStatisticalSummary = Statistics.colStats(observations)
	println(summary.mean) // a dense vector containing the mean value for each column
	println(summary.variance) // column-wise variance
	println(summary.numNonzeros) // number of nonzeros in each column
}