import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._
import com.github.luzhuomi.regex.PDeriv._ // requires sbt assembly



/*
To extract US address from a file of address
*/

object Extract {
	val opat = compile("^(.*) ([A-Za-z]{2}) ([0-9]{5})(-[0-9]{4})?$")
	def main(args: Array[String]) = {

		opat match
		{
			case None    => println("Pattern compilation error." )
			case Some(p) =>
			{
				val conf = new SparkConf().setAppName("ETL (Extract) Example")
				val sc   = new SparkContext(conf)
				// load the file
				val input:RDD[String] = sc.textFile("c:/tmp/input/extract/")

				val extracted = input.map(l => {
						exec(p,l.trim) match
						{
							case Some(env) => List(l,"Y").mkString("\t")
							case None => List(l,"N").mkString("\t")
						}
					})
				extracted.saveAsTextFile(s"c:/tmp/output/extract/")
			}
		}
	}
}

/*
sbt assembly
/opt/spark-1.4.1-bin-hadoop2.6/bin/spark-submit --class Extract target/scala-2.10/spark-examples-assembly-0.1.0.jar
*/
