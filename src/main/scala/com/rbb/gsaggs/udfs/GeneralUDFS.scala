package com.rbb.gsaggs.udfs

import com.google.common.{ geometry => s2 }
import com.rbb.gsaggs.CaseClasses.SimScore
import com.github.halfmatthalfcat.stringmetric.similarity.{
  DiceSorensenMetric,
  JaroMetric,
  NGramMetric,
  OverlapMetric
}
import com.github.halfmatthalfcat.stringmetric.transform.{
  filterAlpha,
  ignoreAlphaCase
}
import org.apache.commons.math3.special.Erf
import org.apache.spark.sql.functions.udf
import scala.collection.JavaConversions._
import scala.math

object GeneralUDFS {
  val flattenUdf = udf((x: Seq[Seq[String]]) => x.flatten.distinct)

  def numericToDouble(
      num: Number
  ): Double = {
    num.asInstanceOf[Number].doubleValue
  }

  def replaceString(
      colValue: String,
      checkValue: String,
      replacementValue: String
  ): String = {
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

  def replaceEmptyStringArray(
      colList: Seq[String],
      replacementValue: String
  ): Seq[String] = {
    if (colList.isEmpty) {
      Seq(replacementValue)
    } else {
      colList
    }
  }

  val replaceEmptyStringArrayUdf = udf((stringArray: Seq[String], replacementValue: String) => replaceEmptyStringArray(stringArray, replacementValue))

  // get /24 block of an IP
  val ipv4Regex = """(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})""".r
  def get_ip_24block(
      ip: String
  ): String = {
    ip match {
      case ipv4Regex(ip1, ip2, ip3, ip4) => s"${ip1}.${ip2}.${ip3}"
      case _ => ""
    }
  }

  // The input ips are ips seperated by ","
  def get_ip_24block_array(
      ips: String
  ): Array[String] = {
    ips.split(",").map(ip => get_ip_24block(ip))
  }

  /*
  *   Uses googles s2 library https://github.com/google/s2-geometry-library-java
  */
  def latLongToCellId(
      lat: Double,
      long: Double,
      level: Int = 5
  ): Long = {
    s2.S2CellId.fromLatLng(s2.S2LatLng.fromDegrees(lat, long)).parent(level).id
  }

  val latLongToCellIdUdf = udf((lat: Double, long: Double) => latLongToCellId(lat, long))

  /*
  * Normalizes an outlier score transforming it into probability. Ranges between [0, 1]
  */
  def outlierScoreGaussianScaler(
      outlierScore: Double,
      sampleMean: Double,
      sampleStddev: Double
  ): Double = {
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
  def outlierScore(
      outlierScores: Seq[Double]
  ): Double = {
    val outlierScoresSansNaN = outlierScores.filter(!_.isNaN)
    val outlierScore = outlierScoresSansNaN.sum / outlierScoresSansNaN.length

    outlierScore
  }

  val outlierScoreUdf = udf((outlierScores: Seq[Double]) => outlierScore(outlierScores))

  def arrayToString(arr: Seq[Any]): String = {
    s"""[${arr.mkString(",")}]"""
  }

  val arrayToStringUdf = udf((arr: Seq[String]) => arrayToString(arr))

  def getFirstArrayElement(
      stringArray: Seq[String]
  ): String = {
    stringArray.head
  }

  def geomMean(
      nums: Iterable[Double]
  ): Double = {
    math.pow(nums.foldLeft(1.0) { _ * _ }, 1.0 / nums.size)
  }

  def getSimScores(
      string1: String,
      string2: String
  ): SimScore = {
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
}