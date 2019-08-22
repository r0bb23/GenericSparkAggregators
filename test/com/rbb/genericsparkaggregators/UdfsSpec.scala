package com.rbb.genericsparkaggregators

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.rbb.genericsparkaggregators.CaseClasses.SimScore
import org.apache.spark.sql.functions.{ callUDF, col, lit }
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class UdfsTest extends FunSuite with DataFrameSuiteBase {
  import spark.implicits._

  test("Gaussian Scaler Positive Score") {
    assert(Udfs.outlierScoreGaussianScaler(2, 0, 1) === 0.9544997361036417)
  }

  test("Gaussian Scaler Zero Score") {
    assert(Udfs.outlierScoreGaussianScaler(0, 0, 1) === 0.0)
  }

  test("Gaussian Scaler Negative Score") {
    assert(Udfs.outlierScoreGaussianScaler(-2, 0, 1) === 0)
  }

  test("Gaussian Scaler High Score") {
    assert(Udfs.outlierScoreGaussianScaler(1000, 0, 1) === 1.0)
  }

  test("Gaussian Scaler NaN Score") {
    assert(Udfs.outlierScoreGaussianScaler(Double.NaN, 0, 1).isNaN)
  }

  test("Outlier Score All NaN") {
    assert(Udfs.outlierScore(List(Double.NaN, Double.NaN, Double.NaN)).isNaN)
  }

  test("Similarity Scores Same") {
    val testSimScores1 = Udfs.getSimScores("test", "test")
    val actualSimScores1 = SimScore(
      dice_sorensen_score = 1,
      geo_mean_score      = 1,
      jaro_score          = 1,
      ngram_score         = 1,
      overlap_score       = 1
    )
    assert(testSimScores1 === actualSimScores1)
  }

  test("Similarity Scores Similar") {
    val testSimScores2 = Udfs.getSimScores("test123", "test456")
    val actualSimScores2 = SimScore(
      dice_sorensen_score = 1,
      geo_mean_score      = 1,
      jaro_score          = 1,
      ngram_score         = 1,
      overlap_score       = 1
    )
    assert(testSimScores2 === actualSimScores2)
  }

  test("Similarity Scores Different") {
    val testSimScores3 = Udfs.getSimScores("test123", "heyhey")
    val actualSimScores3 = SimScore(
      dice_sorensen_score = 0.2,
      geo_mean_score      = 0.2504616824834158,
      jaro_score          = 0.47222222222222215,
      ngram_score         = 0.16666666666666666,
      overlap_score       = 0.25
    )
    assert(testSimScores3 === actualSimScores3)
  }

  test("get_ip_24block") {
    // Test regular ipv4 address
    assert(Udfs.get_ip_24block("10.202.10.95") == "10.202.10")

    // Testa empty ip
    assert(Udfs.get_ip_24block("") == "")

    // Test ipv6 address
    assert(Udfs.get_ip_24block("2001:0db8:85a3:0000:0000:8a2e:0370:7334") == "")
  }

  test("To tdigest UDAF test") {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val tDigest = new Udfs.toTDigest

    val test = sc.parallelize(List[(String, Double)](
      ("user1", 1.0),
      ("user1", 2.0),
      ("user2", 2.0),
      ("user2", 3.0),
      ("user2", 4.0),
      ("user3", 3.0),
      ("user3", 4.0)
    ))
      .toDF("users", "double_col")
      .groupBy("users")
      .agg(tDigest(col("double_col")).as("tdigest"))
      .orderBy(col("users").asc)

    val actual = sc.parallelize(List[(String, Array[Byte])](
      ("user1", Udfs.createTDigest(List(1.0, 2.0))),
      ("user2", Udfs.createTDigest(List(2.0, 3.0, 4.0))),
      ("user3", Udfs.createTDigest(List(3.0, 4.0)))
    )).toDF("users", "tdigest")
      .orderBy(col("users").asc)

    assertDataFrameEquals(test, actual) // equal
  }
}
