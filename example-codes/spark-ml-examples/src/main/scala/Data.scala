import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.mllib.linalg.{Vector, Vectors, Matrix, Matrices}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.distributed.{RowMatrix, IndexedRow, IndexedRowMatrix, CoordinateMatrix, MatrixEntry}

object Data {
	/////////////////////////////////////////////////////////////////////////////////////
	// an RDD[T] is a distributed collection of elements of type T.
	val sc = new SparkContext("local", "shell")
	// an RDD of doubles
	val seriesX:RDD[Double] = sc.textFile("data/basic/series1.txt").map(_.toDouble)
	// collecting RDD data into an local array
	val arr:Array[Double] = seriesX.collect.toArray

	/////////////////////////////////////////////////////////////////////////////////////
	// Local vectors
	/*
	A local vector has integer-typed and 0-based indices and double-typed values,
	stored on a single machine. MLlib supports two types of local vectors: dense and sparse.
	A dense vector is backed by a double array representing its entry values,
	while a sparse vector is backed by two parallel arrays: indices and values.
	For example, a vector (1.0, 0.0, 3.0) can be represented in dense format as [1.0, 0.0, 3.0]
	or in sparse format as (3, [0, 2], [1.0, 3.0]), where 3 is the size of the vector.
	*/
	// Create a dense vector (1.0, 0.0, 3.0).
	val dv: Vector = Vectors.dense(1.0, 0.0, 3.0)
	// Create a sparse vector (1.0, 0.0, 3.0) by specifying its indices and values corresponding to nonzero entries.
	val sv1: Vector = Vectors.sparse(3, Array(0, 2), Array(1.0, 3.0))
	// Create a sparse vector (1.0, 0.0, 3.0) by specifying its nonzero entries.
	val sv2: Vector = Vectors.sparse(3, Seq((0, 1.0), (2, 3.0)))

	/////////////////////////////////////////////////////////////////////////////////////
	// Labeled point
	/*
	A labeled point is a local vector, either dense or sparse, associated with a label/response.
	In MLlib, labeled points are used in supervised learning algorithms. We use a double to store a label,
	so we can use labeled points in both regression and classification. For binary classification,
	a label should be either 0 (negative) or 1 (positive). For multiclass classification,
	labels should be class indices starting from zero: 0, 1, 2, ....
	*/

	// Create a labeled point with a positive label and a dense feature vector.
	val pos = LabeledPoint(1.0, Vectors.dense(1.0, 0.0, 3.0))

	// Create a labeled point with a negative label and a sparse feature vector.
	val neg = LabeledPoint(0.0, Vectors.sparse(3, Array(0, 2), Array(1.0, 3.0)))

	// loading labeledd points from a file
	/*
	It is very common in practice to have sparse training data. MLlib supports reading training examples
	stored in LIBSVM format, which is the default format used by LIBSVM and LIBLINEAR. It is a text format
	in which each line represents a labeled sparse feature vector using the following format:

	label index1:value1 index2:value2 ...
	where the indices are one-based and in ascending order. After loading, the feature indices are converted to zero-based.
	*/
	val examples: RDD[LabeledPoint] = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")

	/////////////////////////////////////////////////////////////////////////////////////
	// Local Matrix
	// a 3 (rows) by 2 (columns) matrix
	val dm: Matrix = Matrices.dense(3, 2, Array(1.0, 3.0, 5.0, 2.0, 4.0, 6.0))


	/////////////////////////////////////////////////////////////////////////////////////
	// Distributed Matrix
	/* The basic type is called RowMatrix. A RowMatrix is a row-oriented distributed matrix without meaningful
	row indices, e.g., a collection of feature vectors. It is backed by an RDD of its rows, where each row is
	a local vector. We assume that the number of columns is not huge for a RowMatrix so that a single local vector
	can be reasonably communicated to the driver and can also be stored / operated on using a single node.
	*/

	val rows: RDD[Vector] = MLUtils.loadVectors(sc, "data/basic/vector1.txt")
	// Create a RowMatrix from an RDD[Vector].
	val mat: RowMatrix = new RowMatrix(rows)
	// Get its size.
	val m = mat.numRows()
	val n = mat.numCols()
	/* An IndexedRowMatrix is similar to a RowMatrix but with meaningful row indices. It is backed by an RDD of
	indexed rows, so that each row is represented by its index (long-typed) and a local vector.
	An IndexedRowMatrix can be created from an RDD[IndexedRow] instance, where IndexedRow is a wrapper over (Long, Vector).
	An IndexedRowMatrix can be converted to a RowMatrix by dropping its row indices.
	*/
	// create indexedRows from rows by zipping with unique Id as the indexes
	val indexedRows : RDD[IndexedRow] = rows.zipWithUniqueId().map( idv => IndexedRow(idv._2,idv._1) )
	// Create an IndexedRowMatrix from an RDD[IndexedRow].
	val irmat: IndexedRowMatrix = new IndexedRowMatrix(indexedRows)

	// Get its size.
	val irm = irmat.numRows()
	val irn = irmat.numCols()

	// Drop its row indices.
	val rowMat: RowMatrix = irmat.toRowMatrix()

	/*
	A CoordinateMatrix is a distributed matrix backed by an RDD of its entries.
	Each entry is a tuple of (i: Long, j: Long, value: Double), where i is the row index,
	j is the column index, and value is the entry value. A CoordinateMatrix should be used only when both
	dimensions of the matrix are huge and the matrix is very sparse.
	*/

	val local_entries : List[MatrixEntry] = (for ( i <- (1 to 10); j <- (2 to 5) ) yield MatrixEntry(i, j, 0.0)).toList
	val entries: RDD[MatrixEntry] = sc.parallelize(local_entries, 2)
	// Create a CoordinateMatrix from an RDD[MatrixEntry].
	val cmat: CoordinateMatrix = new CoordinateMatrix(entries)

	// Get its size.
	val cm = cmat.numRows()
	val cn = cmat.numCols()

	// Convert it to an IndexRowMatrix whose rows are sparse vectors.
	val cindexedRowMatrix = cmat.toIndexedRowMatrix()

}
