package com.rbb.gsaggs.aggregators

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.rbb.gsaggs.CaseClasses.HistogramAndPercentiles
import com.rbb.gsaggs.udfs.TDigestUDFS
import org.apache.spark.SparkException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.scalatest.FunSuite

class TDigestAggsTests extends FunSuite with DataFrameSuiteBase {
  test("To tdigest test nSteps = None") {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

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
      .agg(TDigestAggs.toTDigest("double_col").toColumn.name("tdigest"))
      .orderBy(col("users").asc)

    val actual = sc.parallelize(List[(String, Array[Byte])](
      ("user1", TDigestUDFS.createTDigest(List(1.0, 2.0))),
      ("user2", TDigestUDFS.createTDigest(List(2.0, 3.0, 4.0))),
      ("user3", TDigestUDFS.createTDigest(List(3.0, 4.0)))
    )).toDF("users", "tdigest")
      .orderBy(col("users").asc)

    assertDataFrameEquals(test, actual) // equal
  }

  test("To tdigest test nSteps = 24") {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

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
      .agg(TDigestAggs.toTDigest("double_col", Some(24)).toColumn.name("tdigest"))
      .orderBy(col("users").asc)

    val actual = sc.parallelize(List[(String, Array[Byte])](
      ("user1", TDigestUDFS.createTDigest(List(1.0, 2.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0
        ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0
        ,0.0 ,0.0))),
      ("user2", TDigestUDFS.createTDigest(List(2.0, 3.0, 4.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0
        ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0
        ,0.0 ,0.0))),
      ("user3", TDigestUDFS.createTDigest(List(3.0, 4.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0
        ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0
        ,0.0 ,0.0)))
    )).toDF("users", "tdigest")
      .orderBy(col("users").asc)

    assertDataFrameEquals(test, actual) // equal
  }

  test("To tdigest test nSteps < count") {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    intercept[SparkException] {
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
        .agg(TDigestAggs.toTDigest("double_col", Some(1)).toColumn.name("tdigest"))
        .orderBy(col("users").asc)

      test.first
    }
  }

    test("To percentiles test nSteps = None") {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

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
      .agg(TDigestAggs.toPercentiles("double_col").toColumn.name("tdigest"))
      .orderBy(col("users").asc)

    val actual = sc.parallelize(List[(String, HistogramAndPercentiles)](
      ("user1", TDigestUDFS.toHistogramAndPercentiles(TDigestUDFS.createTDigest(List(1.0, 2.0)))),
      ("user2", TDigestUDFS.toHistogramAndPercentiles(TDigestUDFS.createTDigest(List(2.0, 3.0, 4.0)))),
      ("user3", TDigestUDFS.toHistogramAndPercentiles(TDigestUDFS.createTDigest(List(3.0, 4.0))))
    )).toDF("users", "tdigest")
      .orderBy(col("users").asc)

    assertDataFrameEquals(test, actual) // equal
  }

  test("To percentiles test nSteps = 24") {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

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
      .agg(TDigestAggs.toPercentiles("double_col", Some(24)).toColumn.name("tdigest"))
      .orderBy(col("users").asc)

    val actual = sc.parallelize(List[(String, HistogramAndPercentiles)](
      ("user1", TDigestUDFS.toHistogramAndPercentiles(TDigestUDFS.createTDigest(List(1.0, 2.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0
        ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0
        ,0.0 ,0.0)))),
      ("user2", TDigestUDFS.toHistogramAndPercentiles(TDigestUDFS.createTDigest(List(2.0, 3.0, 4.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0
        ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0
        ,0.0 ,0.0)))),
      ("user3", TDigestUDFS.toHistogramAndPercentiles(TDigestUDFS.createTDigest(List(3.0, 4.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0
        ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0
        ,0.0 ,0.0))))
    )).toDF("users", "tdigest")
      .orderBy(col("users").asc)

    assertDataFrameEquals(test, actual) // equal
  }

  test("To percentiles test nSteps < count") {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    intercept[SparkException] {
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
        .agg(TDigestAggs.toPercentiles("double_col", Some(1)).toColumn.name("tdigest"))
        .orderBy(col("users").asc)

      test.first
    }
  }

  test("Merge tdigest test") {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

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
      .agg(TDigestAggs.toTDigest("double_col").toColumn.name("tdigest"))
      .groupBy()
      .agg(TDigestAggs.mergeTDigests("tdigest").toColumn.name("tdigest"))

    val actual = sc.parallelize(List[Array[Byte]](
      TDigestUDFS.createTDigest(List(1.0, 2.0, 2.0, 3.0, 4.0, 3.0, 4.0))
    )).toDF("tdigest")

    assertDataFrameEquals(test, actual) // equal
  }
}