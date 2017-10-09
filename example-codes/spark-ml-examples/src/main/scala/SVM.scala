import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils

object SVM {
	// def main(args: Array[String]) {
		
		// local mode
		val sc = new SparkContext("local", "shell")	
		// Load training data in LIBSVM format.
		val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")

		// cluster mode
		/*
		val conf = new SparkConf().setAppName("Spark SVM")
	    val sc = new SparkContext(conf)
		val data = MLUtils.loadLibSVMFile(sc, "hdfs://127.0.0.1:9000/data/mllib/sample_libsvm_data.txt")    
		// */

		// Split data into training (60%) and test (40%).
		val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
		val training = splits(0).cache()
		val test = splits(1)

		// Run training algorithm to build the model
		val numIterations = 100
		val model = SVMWithSGD.train(training, numIterations)

		// Clear the default threshold.
		model.clearThreshold()

		// Compute raw scores on the test set. 
		val scoreAndLabels = test.map { point =>
			val score = model.predict(point.features)
			(score, point.label)
		}

		// Get evaluation metrics.
		val metrics = new BinaryClassificationMetrics(scoreAndLabels)
		val auROC = metrics.areaUnderROC()

		println("Area under ROC = " + auROC)	
		sc.stop
	// }
}
