package com.rbb.gsaggs.udfs

import com.rbb.gsaggs.CaseClasses.{
  Histogram,
  HistogramAndPercentiles,
  HistogramAndStats,
  Stats,
}
import com.tdunning.math.stats.{
  MergingDigest,
  TDigest,
}
import java.nio.ByteBuffer
import org.apache.spark.sql.functions.udf
import scala.collection.JavaConverters._
import scala.math

object TDigestUDFS {
  def tdigestArrayToObject(
      tDigestArray: Array[Byte],
  ): TDigest = {
    val buffer = ByteBuffer.allocate(tDigestArray.length)
    buffer.put(ByteBuffer.wrap(tDigestArray))
    buffer.rewind
    val tDigest = MergingDigest.fromBytes(buffer)

    tDigest
  }

  def tdigestKSTest(
      tdigestModelArray: Array[Byte],
      tdigestTestArray:  Array[Byte],
  ): Double = {
    val tdigestModel = tdigestArrayToObject(tdigestModelArray)
    val tdigestTest = tdigestArrayToObject(tdigestTestArray)

    var maxCDFDiff = 0.0
    var n = 0
    tdigestTest.centroids.forEach{
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

  def tdigestCdfScore(
      tdigestModelArray: Array[Byte],
      value:             Double,
  ): Double = {
    val tdigestModel = tdigestArrayToObject(tdigestModelArray)
    val outlierScore = tdigestModel.cdf(value)

    outlierScore
  }

  val tdigestCdfScoreUdf = udf((tdigestModelArray: Array[Byte], value: Double) => tdigestCdfScore(tdigestModelArray, value))

  def getHistogramAndStats(
      tDigest:       TDigest,
      isZeroBounded: Boolean = true,
  ): HistogramAndStats = {
    val histogramArray = for (centroid <- tDigest.centroids.asScala)
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

  def getHistogramAndPercentiles(
      tDigest:       TDigest,
      isZeroBounded: Boolean = true,
  ): HistogramAndPercentiles = {
    val histogramArray = for (centroid <- tDigest.centroids.asScala)
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

  def toHistogramAndStats(
      tDigestArray: Array[Byte],
  ): HistogramAndStats = {
    val buffer = ByteBuffer.allocate(tDigestArray.length)
    buffer.put(ByteBuffer.wrap(tDigestArray))
    buffer.rewind
    val tDigest = MergingDigest.fromBytes(buffer)

    getHistogramAndStats(tDigest: TDigest)
  }

  val toHistogramAndStatsUdf = udf((tDigestArray: Array[Byte]) => toHistogramAndStats(tDigestArray))

  def toHistogramAndPercentiles(
      tDigestArray: Array[Byte],
  ): HistogramAndPercentiles = {
    val buffer = ByteBuffer.allocate(tDigestArray.length)
    buffer.put(ByteBuffer.wrap(tDigestArray))
    buffer.rewind
    val tDigest = MergingDigest.fromBytes(buffer)

    getHistogramAndPercentiles(tDigest: TDigest)
  }

  def createTDigestFromVal[ValueType](
      value: ValueType,
  ): Array[Byte] = {
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

  def createTDigest[DateType](
      data: Seq[DateType],
  ): Array[Byte] = {
    val tDigest = TDigest.createMergingDigest(100)
    data.foreach(value => tDigest.add(value.asInstanceOf[Number].doubleValue))

    val buffer = ByteBuffer.allocate(tDigest.smallByteSize())
    tDigest.asSmallBytes(buffer)

    buffer.array()
  }

  def mergeTDigest(
      tDigests: Seq[Array[Byte]],
  ): Array[Byte] = {
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
}