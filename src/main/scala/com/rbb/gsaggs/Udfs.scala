package com.rbb.gsaggs

import com.google.common.{ geometry => s2 }
import com.rbb.gsaggs.CaseClasses.{ FreqSketch, Histogram, HistogramAndPercentiles, HistogramAndStats, SimScore, Stats }
import com.rbb.gsaggs.ScalaHelpers.{ byteArrayToObject, objectToByteArray }
import com.mozilla.spark.sql.hyperloglog.aggregates.HyperLogLogMerge
import com.mozilla.spark.sql.hyperloglog.functions.{ hllCardinality, hllCreate }
import com.rockymadden.stringmetric.similarity.{ DiceSorensenMetric, JaroMetric, NGramMetric, OverlapMetric }
import com.rockymadden.stringmetric.transform.{ filterAlpha, ignoreAlphaCase }
import com.tdunning.math.stats.{ MergingDigest, TDigest }
import com.twitter.algebird.{ HyperLogLog, HyperLogLogMonoid }
import com.yahoo.memory.Memory
import com.yahoo.sketches.ArrayOfStringsSerDe
import com.yahoo.sketches.frequencies.ItemsSketch
import java.nio.ByteBuffer
import org.apache.commons.math3.special.Erf
import org.apache.spark.sql.{ Row, SparkSession }
import org.apache.spark.sql.expressions.{ MutableAggregationBuffer, UserDefinedAggregateFunction }
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{ BinaryType, DataType, DoubleType, StructField, StructType }
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

object Udfs {
  def registerUDFs(): SparkSession = {
    val spark = SparkSession
      .builder()
      .getOrCreate()

    val hllMerge = new HyperLogLogMerge

    spark.udf.register("array_to_string", arrayToString _)
    spark.udf.register("create_tdigest", createTDigest[Double] _)
    spark.udf.register("double", numericToDouble _)
    spark.udf.register("empty_tdigest", createEmptyTDigest _)
    spark.udf.register("first_array_element", getFirstArrayElement _)
    spark.udf.register("freq_class", FreqSketchUdfs.freqSketchArrayToFreqSketch _)
    spark.udf.register("get_ip_24block", get_ip_24block _)
    spark.udf.register("get_ip_24block_array", get_ip_24block_array _)
    spark.udf.register("histogram_and_percentiles", toHistogramAndPercentiles _)
    spark.udf.register("histogram_and_stats", toHistogramAndStats _)
    spark.udf.register("hll_cardinality", hllCardinality _)
    spark.udf.register("hll_create", hllCreate _)
    spark.udf.register("hll_merge", hllMerge)
    spark.udf.register("replace_empty_string_array", replaceEmptyStringArray _)
    spark.udf.register("replace_string", replaceString _)
    spark.udf.register("sim_scores", getSimScores _)
    spark.udf.register("tdigest", new toTDigest)
    
    spark
  }

  // https://github.com/vitillo/spark-hyperloglog/blob/master/src/main/scala/com/mozilla/spark/sql/hyperloglog/functions/package.scala
  val hllCreateUdf = udf((s: String, b: Int) => hllCreate(s, b))

  val flattenUdf = udf((x: Seq[Seq[String]]) => x.flatten.distinct)

  def hllConverter(data: Either[List[Int], List[String]]): Array[Byte] = {
    val hllMonoid = new HyperLogLogMonoid(bits = 12)
    val hlls = data match {
      case Left(dataInt) => dataInt.map { x => hllMonoid.create(HyperLogLog.toBytes(hllMonoid.toHLL(x))) }
      case Right(dataString) => dataString.map { x => hllMonoid.create(HyperLogLog.toBytes(hllMonoid.toHLL(x))) }
    }
    val combinedHll = hllMonoid.sum(hlls)
    HyperLogLog.toBytes(combinedHll)
  }

  def numericToDouble(num: Number): Double = {
    num.asInstanceOf[Number].doubleValue
  }

  def replaceString(colValue: String, checkValue: String, replacementValue: String): String = {
    val trimedValue = if (colValue != null){
      colValue.trim
    }
    else {
      ""
    }
    
    if (trimedValue == checkValue) {
      replacementValue
    } else {
      colValue
    }
  }

  def replaceEmptyStringArray(colList: Seq[String], replacementValue: String): Seq[String] = {
    if (colList.isEmpty) {
      Seq(replacementValue)
    } else {
      colList
    }
  }

  val replaceEmptyStringArrayUdf = udf((stringArray: Seq[String], replacementValue: String) => replaceEmptyStringArray(stringArray, replacementValue))

  // get /24 block of an IP
  val ipv4Regex = """(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})""".r
  def get_ip_24block(ip: String): String = {
    ip match {
      case ipv4Regex(ip1, ip2, ip3, ip4) => s"${ip1}.${ip2}.${ip3}"
      case _ => ""
    }
  }

  // The input ips are ips seperated by ","
  def get_ip_24block_array(ips: String): Array[String] = {
    ips.split(",").map(ip => get_ip_24block(ip))
  }

  /*
  *   Uses googles s2 library https://github.com/google/s2-geometry-library-java
  */
  def latLongToCellId(lat: Double, long: Double, level: Int = 5): Long = {
    s2.S2CellId.fromLatLng(s2.S2LatLng.fromDegrees(lat, long)).parent(level).id
  }

  val latLongToCellIdUdf = udf((lat: Double, long: Double) => latLongToCellId(lat, long))

  def tdigestArrayToObject(tDigestArray: Array[Byte]): TDigest = {
    val buffer = ByteBuffer.allocate(tDigestArray.length)
    buffer.put(ByteBuffer.wrap(tDigestArray))
    buffer.rewind
    val tDigest = MergingDigest.fromBytes(buffer)

    tDigest
  }

  def tdigestKSTest(tdigestModelArray: Array[Byte], tdigestTestArray: Array[Byte]): Double = {
    val tdigestModel = tdigestArrayToObject(tdigestModelArray)
    val tdigestTest = tdigestArrayToObject(tdigestTestArray)

    var maxCDFDiff = 0.0
    var n = 0
    tdigestTest.centroids.foreach{
      centroid => {
        n += 1
        maxCDFDiff = math.max(maxCDFDiff, math.abs(tdigestModel.cdf(centroid.mean) - centroid.count.toDouble))
      }
    }
    
    val outlierScore = if (n > 2) {
      maxCDFDiff
    } else {
      Double.NaN
    }

    outlierScore
  }

  val tdigestKSTestUdf = udf((tdigestModelArray: Array[Byte], tdigestTestArray: Array[Byte]) => tdigestKSTest(tdigestModelArray, tdigestTestArray))

  def tdigestCdfScore(tdigestModelArray: Array[Byte], value: Double): Double = {
    val tdigestModel = tdigestArrayToObject(tdigestModelArray)
    val outlierScore = tdigestModel.cdf(value)

    outlierScore
  }

  val tdigestCdfScoreUdf = udf((tdigestModelArray: Array[Byte], value: Double) => tdigestCdfScore(tdigestModelArray, value))

  /*
  * Normalizes an outlier score transforming it into probability. Ranges between [0, 1]
  */
  def outlierScoreGaussianScaler(outlierScore: Double, sampleMean: Double, sampleStddev: Double): Double = {
    val normalizedOutlierScore = (outlierScore - sampleMean) / (sampleStddev * math.sqrt(2))
    val erfScore = Erf.erf(normalizedOutlierScore)

    if (erfScore > 0) {
      erfScore
    } else if (erfScore.isNaN) {
      Double.NaN
    } else {
      0.0
    }
  }

  val outlierScoreGaussianScalerUdf = udf((outlierScore: Double, sampleMean: Double, sampleStddev: Double) => outlierScoreGaussianScaler(outlierScore, sampleMean, sampleStddev))

  /*
  * Averages outlier scores.
  */
  def outlierScore(outlierScores: Seq[Double]): Double = {
    val outlierScoresSansNaN = outlierScores.filter(!_.isNaN)
    val outlierScore = outlierScoresSansNaN.sum / outlierScoresSansNaN.length

    outlierScore
  }

  val outlierScoreUdf = udf((outlierScores: Seq[Double]) => outlierScore(outlierScores))

  def arrayToString(arr: Seq[Any]): String = {
    s"""[${arr.mkString(",")}]"""
  }
  
  val arrayToStringUdf = udf((arr: Seq[String]) => arrayToString(arr))

  def getFirstArrayElement(stringArray: Seq[String]): String = {
    stringArray.head
  }

  def getHistogramAndStats(tDigest: TDigest, isZeroBounded: Boolean = true): HistogramAndStats = {
    val histogramArray = for (centroid <- tDigest.centroids)
      yield Histogram(
      centroid = centroid.mean,
      count    = centroid.count,
      sum      = centroid.mean * centroid.count
    )
    val globalSum = histogramArray.foldLeft(0.0)((sum, centroid) => sum + centroid.sum)
    val iqr = tDigest.quantile(0.75) - tDigest.quantile(0.25)
    val iqr1_5 = 1.5 * iqr
    val q1 = tDigest.quantile(0.25)
    val q3 = tDigest.quantile(0.75)
    val temp_lower_whisker = q1 - iqr1_5
    val lower_whisker = if ((temp_lower_whisker <= 0.0) && isZeroBounded) {
        0.0
      } else {
        temp_lower_whisker
      }

    HistogramAndStats(
      buckets = histogramArray.toArray,
      stats   = Stats(
        count = tDigest.size,
        iqr   = iqr,
        // http://mathworld.wolfram.com/Box-and-WhiskerPlot.html (first method descriped on this page.)
        // Whiskers are calculated using the typical iqr * 1.5 method:
        lower_whisker = lower_whisker,
        max           = tDigest.getMax,
        mean          = globalSum / tDigest.size,
        median        = tDigest.quantile(0.50),
        min           = tDigest.getMin,
        percentile_05 = tDigest.quantile(0.05),
        percentile_25 = q1,
        percentile_75 = q3,
        percentile_95 = tDigest.quantile(0.95),
        sum           = globalSum,
        upper_whisker = q3 + iqr1_5
      )
    )
  }

  def getHistogramAndPercentiles(tDigest: TDigest, isZeroBounded: Boolean = true): HistogramAndPercentiles = {
    val histogramArray = for (centroid <- tDigest.centroids)
      yield Histogram(
      centroid = centroid.mean,
      count    = centroid.count,
      sum      = centroid.mean * centroid.count
    )
    val q1 = tDigest.quantile(0.25)
    val q3 = tDigest.quantile(0.75)
    val iqr = q3 - q1
    val iqr1_5 = 1.5 * iqr
    val temp_lower_whisker = q1 - iqr1_5
    val lower_whisker = if ((temp_lower_whisker <= 0.0) && isZeroBounded) {
        0.0
      } else {
        temp_lower_whisker
      }

    HistogramAndPercentiles(
      buckets = histogramArray.toArray,
      iqr     = iqr,
      // http://mathworld.wolfram.com/Box-and-WhiskerPlot.html (first method descriped on this page.)
      // Whiskers are calculated using the typical iqr * 1.5 method:
      lower_whisker = lower_whisker,
      median        = tDigest.quantile(0.50),
      percentile_05 = tDigest.quantile(0.05),
      percentile_25 = q1,
      percentile_75 = q3,
      percentile_95 = tDigest.quantile(0.95),
      upper_whisker = q3 + iqr1_5
    )
  }

  def toHistogramAndStats(tDigestArray: Array[Byte]): HistogramAndStats = {
    val buffer = ByteBuffer.allocate(tDigestArray.length)
    buffer.put(ByteBuffer.wrap(tDigestArray))
    buffer.rewind
    val tDigest = MergingDigest.fromBytes(buffer)

    getHistogramAndStats(tDigest: TDigest)
  }

  val toHistogramAndStatsUdf = udf((tDigestArray: Array[Byte]) => toHistogramAndStats(tDigestArray))

  def toHistogramAndPercentiles(tDigestArray: Array[Byte]): HistogramAndPercentiles = {
    val buffer = ByteBuffer.allocate(tDigestArray.length)
    buffer.put(ByteBuffer.wrap(tDigestArray))
    buffer.rewind
    val tDigest = MergingDigest.fromBytes(buffer)

    getHistogramAndPercentiles(tDigest: TDigest)
  }

  def createTDigestFromVal[T](value: T): Array[Byte] = {
    val tDigest = TDigest.createMergingDigest(100)
    tDigest.add(value.asInstanceOf[Number].doubleValue)

    val buffer = ByteBuffer.allocate(tDigest.smallByteSize())
    tDigest.asSmallBytes(buffer)

    buffer.array()
  }

  def createEmptyTDigest(): Array[Byte] = {
    val tDigest = TDigest.createMergingDigest(100)
    val buffer = ByteBuffer.allocate(tDigest.smallByteSize())
    tDigest.asSmallBytes(buffer)

    buffer.array()
  }

  def createTDigest[T](data: Seq[T]): Array[Byte] = {
    val tDigest = TDigest.createMergingDigest(100)
    data.foreach(value => tDigest.add(value.asInstanceOf[Number].doubleValue))

    val buffer = ByteBuffer.allocate(tDigest.smallByteSize())
    tDigest.asSmallBytes(buffer)

    buffer.array()
  }

  def mergeTDigest(tDigests: Seq[Array[Byte]]): Array[Byte] = {
    val mergedTDigest = TDigest.createMergingDigest(100)
    tDigests.foreach {
      tDigest =>
        {
          val buffer = ByteBuffer.allocate(tDigest.length)
          buffer.put(ByteBuffer.wrap(tDigest))
          buffer.rewind
          mergedTDigest.add(MergingDigest.fromBytes(buffer))
        }
    }

    val buffer = ByteBuffer.allocate(mergedTDigest.smallByteSize())
    mergedTDigest.asSmallBytes(buffer)

    buffer.array()
  }

  val udfTdigestCreate = udf((v: Double) => createTDigestFromVal(v))

  def geomMean(nums: Iterable[Double]): Double = {
    math.pow(nums.foldLeft(1.0) { _ * _ }, 1.0 / nums.size)
  }

  def getSimScores(string1: String, string2: String): SimScore = {
    val composedTransform = (filterAlpha andThen ignoreAlphaCase)

    val scoreMap = Map(
      "diceSorensenScore" -> (DiceSorensenMetric(1) withTransform composedTransform).compare(string1, string2).getOrElse(0.01),
      "jaroScore" -> (JaroMetric withTransform composedTransform).compare(string1, string2).getOrElse(0.01),
      "ngramScore" -> (NGramMetric(1) withTransform composedTransform).compare(string1, string2).getOrElse(0.01),
      "overlapScore" -> (OverlapMetric(1) withTransform composedTransform).compare(string1, string2).getOrElse(0.01)
    )

    SimScore(
      dice_sorensen_score = scoreMap.getOrElse("diceSorensenScore", 0.01),
      geo_mean_score      = geomMean(scoreMap.values),
      jaro_score          = scoreMap.getOrElse("jaroScore", 0.01),
      ngram_score         = scoreMap.getOrElse("ngramScore", 0.01),
      overlap_score       = scoreMap.getOrElse("overlapScore", 0.01)
    )
  }

  class toTDigest extends UserDefinedAggregateFunction {
    // This is the input fields for your aggregate function.
    override def inputSchema: org.apache.spark.sql.types.StructType =
      StructType(StructField("value", DoubleType) :: Nil)

    // This is the internal fields you keep for computing your aggregate.
    override def bufferSchema: StructType =
      StructType(StructField("tdigest", BinaryType) :: Nil)

    // This is the output type of your aggregatation function.
    override def dataType: DataType = BinaryType

    override def deterministic: Boolean = false

    // This is the initial value for your buffer schema.
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = objectToByteArray[TDigest](TDigest.createMergingDigest(100))
    }

    // This is how to update your buffer schema given an input.
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      val tdigest = byteArrayToObject[TDigest](buffer.getAs[Array[Byte]](0))
      tdigest.add(input.getAs[Double](0))
      buffer(0) = objectToByteArray[TDigest](tdigest)
    }

    // This is how to merge two objects with the bufferSchema type.
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      val tdigest1 = byteArrayToObject[TDigest](buffer1.getAs[Array[Byte]](0))
      val tdigest2 = byteArrayToObject[TDigest](buffer2.getAs[Array[Byte]](0))
      tdigest1.add(tdigest2)
      buffer1(0) = objectToByteArray[TDigest](tdigest1)
    }

    // This is where you output the final value, given the final value of your bufferSchema.
    override def evaluate(buffer: Row): Any = {
      val tdigest = byteArrayToObject[TDigest](buffer.getAs[Array[Byte]](0))
      val tdigestBuffer = ByteBuffer.allocate(tdigest.smallByteSize())
      tdigest.asSmallBytes(tdigestBuffer)
      tdigestBuffer.array()
    }
  }
}

object FreqSketchUdfs {
  // Decides what type of error to NOT favor when estimating value counts.
  val errorType = com.yahoo.sketches.frequencies.ErrorType.NO_FALSE_NEGATIVES

  def freqEuclidDist(freqModelBinary: Array[Byte], freqTestBinary: Array[Byte]): Double = {
    def calcEuclidDist(
      freqMerged: ItemsSketch[String],
      freqModel: ItemsSketch[String],
      freqTest: ItemsSketch[String]
    ): Double = {
      val freqModelItems = freqModel.getFrequentItems(errorType)
      val freqTestItems = freqTest.getFrequentItems(errorType)
      val freqMergedItems = freqMerged.getFrequentItems(errorType)
      val freqModelEstimatesSum = freqModelItems.foldLeft(0L) { (sum, row) => sum + row.getEstimate }.toDouble
      val freqTestEstimatesSum = freqTestItems.foldLeft(0L) { (sum, row) => sum + row.getEstimate }.toDouble

      var freqSquaredSum = 0.0
      freqMergedItems.foreach{
        row => {
          val modelRowValue = freqModel.getEstimate(row.getItem) / freqModelEstimatesSum
          val testRowValue = freqTest.getEstimate(row.getItem) / freqTestEstimatesSum
          freqSquaredSum += math.pow(modelRowValue - testRowValue, 2)
        }
      }

      math.pow(freqSquaredSum, 0.5)
    }

    val freqModel = ItemsSketch.getInstance(Memory.wrap(freqModelBinary), new ArrayOfStringsSerDe())
    val freqTest = ItemsSketch.getInstance(Memory.wrap(freqTestBinary), new ArrayOfStringsSerDe())

    val freqMerged = new ItemsSketch[String](math.max(freqModel.getMaximumMapCapacity * 4 / 3, freqTest.getMaximumMapCapacity * 4 / 3))
    freqMerged.merge(freqModel).merge(freqTest)

    calcEuclidDist(freqMerged, freqModel, freqTest)
  }

  val freqEuclidDistUdf = udf((freqModelBinary: Array[Byte], freqTestBinary: Array[Byte]) => freqEuclidDist(freqModelBinary, freqTestBinary))

  def freqSketchToFreqSketch(freq: ItemsSketch[String]): List[FreqSketch] = {
    val freqItems = freq.getFrequentItems(errorType)
    val freqEstimatesSum = freqItems.foldLeft(0L) { (sum, row) => sum + row.getEstimate }.toDouble
  
    val freqs = freqItems.map {
      row =>
        FreqSketch(
          value   = row.getItem,
          freq    = row.getEstimate,
          freq_05 = row.getLowerBound,
          freq_95 = row.getUpperBound,
          percent = row.getEstimate / freqEstimatesSum
        )
    }
    
    freqs.toList
  }

  def freqSketchArrayToFreqSketch(freqArray: Array[Byte]): List[FreqSketch] = {
    val freq = ItemsSketch.getInstance(Memory.wrap(freqArray), new ArrayOfStringsSerDe())
    freqSketchToFreqSketch(freq)
  }

  val freqSketchArrayToFreqSketchUdf = udf((freqArray: Array[Byte]) => freqSketchArrayToFreqSketch(freqArray))
}