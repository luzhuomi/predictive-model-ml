import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd._

import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils

object TweetSVMFilter {

	val vector_fixed_size = 30 // fixed the size of each vector.
	// if vectors have different sizes, the gradient descent algorithm will fail
	// cut off if it exceeds, pad zeros if it has less than 30 elements

	def hash(str:String):Int = str.toList.foldLeft(2147483647)((h,c) => 31*h + c)

	def to_words(tweet:String):List[String] = tweet.split(" ").toList

	def pad_cap(xs:List[Double],size:Int):List[Double] = xs match
	{
		case Nil if size > 0 => List.fill(size)(0.0) // fill the rest with 0.0
		case Nil             => Nil
		case (y::ys) if size == 0 => Nil
		case (y::ys)              => y::(pad_cap(ys,size-1))
	}

	def main(args: Array[String]) {

		// local mode

		val sc = new SparkContext("local", "shell")
		// Load training data in LIBSVM format.
		val posTXT:RDD[String] = sc.textFile("data/tweet/label_data/Kpop/*.txt") // .sample(false,0.1)
		val negTXT:RDD[String] = sc.textFile("data/tweet/label_data/othertweet/*.txt") // .sample(false,0.1)


		// cluster mode
		// val conf = new SparkConf().setAppName("Spark SVM")
	    // val sc = new SparkContext(conf)
		// val posTXT:RDD[String] = sc.textFile("c:/tmp/input/data/tweet/label_data/Kpop/*.txt") // .sample(false,0.1)
		// val negTXT:RDD[String] = sc.textFile("c:/tmp/input/data/tweet/label_data/othertweet/*.txt") // .sample(false,0.1)



		// convert the training data to labeled points
		val posLP:RDD[LabeledPoint] = posTXT.map( (twt:String) =>
		{
			val ws = to_words(twt).map(w => hash(w).toDouble)
			LabeledPoint(1.0, Vectors.dense(pad_cap(ws ,vector_fixed_size).toArray))
		})
		val negLP:RDD[LabeledPoint] = negTXT.map( (twt:String) =>
		{
			val ws = to_words(twt).map(w => hash(w).toDouble)
			LabeledPoint(0.0, Vectors.dense(pad_cap(ws ,vector_fixed_size).toArray))
		})
		val data = negLP ++ posLP


		// Split data into training (60%) and test (40%).
		val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
		val training = splits(0).cache()
		val test = splits(1)

		// Run training algorithm to build the model
		val numIterations = 10
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
	}
}
