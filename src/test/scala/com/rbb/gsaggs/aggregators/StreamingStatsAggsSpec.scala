package com.rbb.gsaggs.aggregators

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.rbb.gsaggs.udfs._
import org.apache.spark.SparkException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.scalatest.FunSuite

class StreamingStatsAggsTests extends FunSuite with DataFrameSuiteBase {
  test("StddevStats test nsteps = None") {
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
      .groupBy()
      .agg(StreamingStatsAggs.toStddevStats("double_col").toColumn.name("tsstats"))
      .select("tsstats.*")
      .first
      .toSeq

    val actual = List(7, 7.428571428571428, 4.0, 2.7142857142857144, 1.0, 19.0)

    assert(test == actual)
  }

  test("StddevStats test nsteps = 24") {
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
      .groupBy()
      .agg(StreamingStatsAggs.toStddevStats("double_col", Some(24)).toColumn.name("tsstats"))
      .select("tsstats.*")
      .first
      .toSeq

    val actual = List(24, 43.958333333333336, 4.0, 0.7916666666666666, 0.0, 19.0)

    assert(test == actual)
  }

  test("StddevStats test nsteps < count") {
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
        .groupBy()
        .agg(StreamingStatsAggs.toStddevStats("double_col", Some(5)).toColumn.name("tsstats"))
        .select("tsstats.*")
        .first
        .toSeq
    }
  }
}