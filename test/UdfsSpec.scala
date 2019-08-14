package com.ionic.helperfunctions

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.ionic.helperfunctions.CaseClasses.SimScore
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

  test("get_attribute_type") {
    Udfs.registerUDFs()

    val inputDF = Seq(
      ("ionic-protected-test1", "value1"),
      ("ionic-plaintext", "value2"),
      // ~!{KEYTAG}~fEc!{IV}{CIPHERTEXT}!cEf
      ("ionic-filename-v1", "~!L4R343NuJl0~fEc!X3ZoW9AeHVaOHfQfEg/IdrjA!cEf"),
      // ~!2!{KEYTAG}!{IV}{CIPHERTEXT}!
      ("ionic-filename-v2", "~!2!E8n5Hnsoli0!7BTjVtTq8McF0bh1sTVvpWma8ik!"),
      // ~!3!{KEYTAG}!{IV}{CIPHERTEXT}{ATAG}!
      ("ionic-filename-v3", "~!3!L4R343NuJl0!X3ZoW9AeHVaOHfQfEg/IdrjA!"),
      // ~\{KEYTAG};{IV}{CIPHERTEXT}\
      ("ionic-filename-v4", "~\\L4R343NuJl0;X3ZoW9AeHVaOHfQfEg\\"),
      ("attr1", "~!2!LYTe4X_V92I! XdkHV6DxVABl5IO MmnuTQrcUSURvl Oe0LpXE7mZ1Foc=!")
    ).toDF("key", "value")
    inputDF.createOrReplaceTempView("temp_data")
    val q = """
        SELECT *, get_attribute_type(key, value) As type
        FROM temp_data
      """
    val outputDF = spark.sql(q)

    //check if  ionic-protected attribute is recognized as IONIC_PROTECTED
    assert(
      outputDF.filter("key like 'ionic-protected-%'").select("type").head.get(0),
      "enc:ionic-protected"
    )

    // check that ionic-filenames are recognized as IONIC_ENCRYPTED, when encrypted using various formats
    assert(
      outputDF.filter("key == 'ionic-filename-v1'").select("type").first.get(0),
      "enc:ionic-encrypted:chunkv1"
    )
    assert(
      outputDF.filter("key == 'ionic-filename-v2'").select("type").first.get(0),
      "enc:ionic-encrypted:chunkv2"
    )
    assert(
      outputDF.filter("key == 'ionic-filename-v3'").select("type").first.get(0),
      "enc:ionic-encrypted:chunkv3"
    )
    assert(
      outputDF.filter("key == 'ionic-filename-v4'").select("type").first.get(0),
      "enc:ionic-encrypted:chunkv4"
    )

    // check if ionic-plaintext is recognized  as PLAINTEXT
    assert(
      outputDF.filter("key == 'ionic-plaintext'").select("type").first.get(0),
      "enc:plaintext"
    )

    // check if attr1 is also recognized PLAINTEXT
    assert(
      outputDF.filter("key == 'attr1'").select("type").first.get(0),
      "enc:plaintext"
    )
  }
}
