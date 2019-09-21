package com.rbb.gsaggs

/*
Also see built ins:

https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/TestUtils.scala
https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/util/Utils.scala

sbt help
> https://www.scala-sbt.org/1.x/docs/Running.html#Common+commands
- launch with `sbt` in directoru with build.sbt
- `test` for tests
- `reload` if any changes to `build.sbt`
- `update` to rebuild after reload

scalatest
> http://www.scalatest.org/user_guide/using_assertions


https://spark.apache.org/docs/latest/sql-programming-guide.html#hive-tables
- spark.sql.warehouse.dir to set up hive table location

*/
import breeze.linalg.DenseMatrix
import breeze.stats.distributions.{ Beta, Gaussian, RandBasis }
import collection.JavaConverters._
import com.rbb.gsaggs.ScalaHelpers.{ byteArrayToObject, objectToByteArray }
import com.tdunning.math.stats.{ MergingDigest, TDigest }
import com.yahoo.sketches.ArrayOfStringsSerDe
import com.yahoo.sketches.frequencies.ItemsSketch
import java.io._
import java.io.BufferedInputStream
import java.io.File
import java.nio.ByteBuffer
import java.nio.file.{ FileSystems, Paths, Path, Files }
import java.util.{ UUID, List => JList }
import java.util.function.{ Function => JFunction, Predicate => JPredicate, BiPredicate }
import java.util.stream.Collectors
import java.util.zip.GZIPInputStream
import org.apache.commons.math3.stat.Frequency
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.{ Dataset, Row, SparkSession }
import org.apache.spark.sql.types.{ DataType, StructType }
import org.scalatest._

/* Helpers for loading resource files as test fixtures
 *
 * Based off:
 * https://databaseline.bitbucket.io/a-quickie-on-reading-json-resource-files-in-apache-spark/
 *
 * Updated to use these methods:
 * https://spark.apache.org/docs/2.3.0/api/java/org/apache/spark/sql/DataFrameReader.html
 */
object ResourceHelpers {
  def readResourcePerLine(file: String)(implicit ss: SparkSession): Dataset[String] = {
    import ss.implicits._
    val stream = this.getClass.getResourceAsStream(s"/$file")
    scala.io.Source.fromInputStream(stream)
      .getLines
      .toList
      .toDS
  }

  def readResourceAsString(file: String): String = {
    val stream = this.getClass.getResourceAsStream(s"/$file")
    scala.io.Source.fromInputStream(stream)
      .mkString
  }

  def readCSVResourceAsDataset(file: String)(implicit ss: SparkSession, sc: SparkContext): Dataset[Row] = {
    ss.read.csv(readResourcePerLine(file))
  }

  def readCSVResourceAsDataFrame(file: String, header: String = "true")(implicit ss: SparkSession): Dataset[Row] = {
    val urlPath = this.getClass.getResource(s"/$file").toString
    val path = java.net.URLDecoder.decode(urlPath, "UTF-8")
    ss.read.format("csv").option("header", header).load(path)
  }

  /**
    * Read from a parquet file and apply the schema
    */
  def readParquetResourceAsDataFrame(dataFile: String, schemaJsonFile: String = null, compressionType: String = "snappy")(implicit ss: SparkSession): Dataset[Row] = {
    val urlPath = this.getClass.getResource(s"/$dataFile").toString
    val path = java.net.URLDecoder.decode(urlPath, "UTF-8")
    var df = ss.read.format("parquet").option("compression", compressionType).load(path)
    if (schemaJsonFile != null) {
      val schemaStr = readResourceAsString(schemaJsonFile)
      val schema = DataType.fromJson(schemaStr).asInstanceOf[StructType]
      df = ss.createDataFrame(df.rdd, schema)
    }

    return df
  }

  def readJsonResourceAsDataset(file: String)(implicit ss: SparkSession, sc: SparkContext): Dataset[Row] = {
    ss.read.json(readResourcePerLine(file))
  }
}

// Mirrored from spark, since they made the urils object private
// https://github.com/apache/spark/blob/master/core/src/test/scala/org/apache/spark/FileSuite.scala#L39
// https://github.com/apache/spark/blob/branch-2.2/core/src/main/scala/org/apache/spark/util/collection/Utils.scala#L27
// https://www.mail-archive.com/user@spark.apache.org/msg52909.html
// http://www.scalatest.org/user_guide/sharing_fixtures
/*

// Add this function to your class that uses this trait if you want to create a local directory that is left around for debugging

    override def getTmpDir():File = {
        //clearAfterTest = true
        new File(".", "tmp")
    }
*/
trait TempDir extends BeforeAndAfterEach { this: Suite =>
  var tempDir: File = _
  var clearAfterTest: Boolean = false

  // Create a function so we can quickly override
  // If the user simply returns a `File` object, it will stick around for debugging
  def getTmpDir(): File = {
    clearAfterTest = true
    Utils.createTempDir()
  }

  override def beforeEach() {
    super.beforeEach()
    tempDir = getTmpDir()
  }

  override def afterEach() {
    try {
      if (clearAfterTest) {
        Utils.deleteRecursively(tempDir)
      }
    }
    finally {
      super.afterEach()
    }
  }
}

/**
  * Collection of utilities modeled off spark utils.Utils
  *
  * Based off: https://github.com/apache/spark/blob/branch-2.2/core/src/main/scala/org/apache/spark/util/Utils.scala#L308
  * We cannot access these directly since they are private
  *
  * The lib `spark-testing-base` takes this same approach:
  * https://github.com/holdenk/spark-testing-base/blob/master/src/main/1.3/scala/com/holdenkarau/spark/testing/Utils.scala
  *
  * Helpful links:
  * https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/TestUtils.scala
  * https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/util/Utils.scala
  *
  */
object Utils {
  /**
    * Create a temporary directory with a given prefix and root
    *
    * Based off: https://github.com/apache/spark/blob/branch-2.2/core/src/main/scala/org/apache/spark/util/Utils.scala#L308
    * Our version combined the functionality of createDirectory w/o retries
    */
  def createTempDir(root: String = System.getProperty("java.io.tmpdir"), namePrefix: String = "ionic-spark"): File = {
    val dir = new File(root, namePrefix + "-" + UUID.randomUUID.toString)
    dir.getCanonicalFile
  }

  /**
    * Fully delete dir
    *
    * Based off: https://github.com/apache/spark/blob/branch-2.2/core/src/main/scala/org/apache/spark/util/Utils.scala#L1009
    */
  def deleteRecursively(file: File) {
    if (file != null) {
      try {
        if (file.isDirectory && !isSymlink(file)) {
          var savedIOException: IOException = null
          for (child <- listFilesSafely(file)) {
            try {
              deleteRecursively(child)
            }
            catch {
              // In case of multiple exceptions, only last one will be thrown
              case ioe: IOException => savedIOException = ioe
            }
          }
          if (savedIOException != null) {
            throw savedIOException
          }
        }
      }
      finally {
        if (!file.delete()) {
          // Delete can also fail if the file simply did not exist
          if (file.exists()) {
            throw new IOException("Failed to delete: " + file.getAbsolutePath)
          }
        }
      }
    }
  }

  /**
    * List. Thow exceptions on null. No errors on non-existing files.
    *
    * Based off: https://github.com/apache/spark/blob/branch-2.2/core/src/main/scala/org/apache/spark/util/Utils.scala#L1041
    */
  def listFilesSafely(file: File): Seq[File] = {
    if (file.exists()) {
      val files = file.listFiles()
      if (files == null) {
        throw new IOException("Failed to list files for dir: " + file)
      }
      files
    }
    else {
      List()
    }
  }

  /**
    * Verify if a file is a symlink
    *
    * Based off: https://github.com/apache/spark/blob/branch-2.2/core/src/main/scala/org/apache/spark/util/Utils.scala#L1041
    */
  def isSymlink(file: File): Boolean = {
    return Files.isSymbolicLink(Paths.get(file.toURI))
  }

  /**
    * For fixing errors in filter lambda functions in java8 stream operations
    * http://www.michaelpollmeier.com/2014/10/12/calling-java-8-functions-from-scala
    *
    * Should be able to remove this when we upgrade to 2.12:
    * http://www.scala-lang.org/news/2.12.0/#lambda-syntax-for-sam-types
    *
    * Possible alt approach?
    * https://github.com/scala/scala-java8-compat
    *
    */
  def toJavaFunction[A, B](f: Function1[A, B]) = new JFunction[A, B] {
    override def apply(a: A): B = f(a)
  }

  def toJavaPredicate[A](f: Function1[A, Boolean]) = new JPredicate[A] {
    override def test(a: A): Boolean = f(a)
  }

  // Get files matching a given predicate
  // This is a little complex now because of issues with java8 functions
  // Based off: https://stackoverflow.com/questions/29574167/how-to-use-files-walk-to-get-a-graph-of-files-based-on-conditions
  // Need to cast to java predicate otherwise we run into issues here.  This same thing works in with scala 2.12 w/o the cast.
  // Note that this also doesn't take a filesystem, so doesn't work for HDFS.
  def getMatchingFiles(basedir: Path, filterFn: (Path) => Boolean): List[Path] = {
    Files.walk(basedir.normalize)
      .filter(Utils.toJavaPredicate(filterFn))
      .collect(Collectors.toList())
      .asScala
      .toList
  }
}

object TestHelpers {
  def toFrequency(keys: Seq[String]): Array[Byte] = {
    val freq = new Frequency()
    keys.foreach(key => freq.addValue(key))

    objectToByteArray(freq)
  }
  
  def toSketchFrequency(keys: Seq[String]): Array[Byte] = {
    val freq = new ItemsSketch[String](512)
    keys.foreach(key => freq.update(key))

    freq.toByteArray(new ArrayOfStringsSerDe())
  }

  def toTdigest(values: Seq[Double]): Array[Byte] = {
    val tdigest = TDigest.createMergingDigest(100)
    values.foreach(value => tdigest.add(value))

    val tdigestBuffer = ByteBuffer.allocate(tdigest.smallByteSize())
    tdigest.asSmallBytes(tdigestBuffer)
    tdigestBuffer.array()
  }

  def randGauss(sampleSize: Int = 50, mu: Int = 0, sigma: Int = 1, seed: Int = 1234): Seq[Double] = {
    new Gaussian(mu, sigma)(RandBasis.withSeed(seed)).sample(sampleSize).toSeq
  }

  def randBeta(sampleSize: Int = 50, alpha: Double = 2.0, beta: Double = 2.0, seed: Int = 1234): Seq[Double] = {
    new Beta(alpha, beta)(RandBasis.withSeed(seed)).sample(sampleSize).toSeq
  }

  def randFreqBeta(strings: List[String], sampleSize: Int = 50, alpha: Double = 2.0, beta: Double = 2.0, seed: Int = 1234): Seq[String] = {
    val betaSeq = randBeta(sampleSize = sampleSize, seed = seed)

    betaSeq.map(x => strings(math.floor(x * strings.length).toInt))
  }
}
