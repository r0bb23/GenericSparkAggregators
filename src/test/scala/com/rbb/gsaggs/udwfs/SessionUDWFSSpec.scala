package com.rbb.gsaggs.udwfs

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.rbb.gsaggs.udfs._
import com.rbb.gsaggs.udwfs._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.scalatest.FunSuite

class SessionUDWFSTests extends FunSuite with DataFrameSuiteBase {
  test("UDWF for sessionization") {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val lagWindow = Window.partitionBy("users").orderBy("epoch_seconds")

    val test = sc.parallelize(List[(String, Long)](
      ("user1", 1525231800),
      ("user1", 1525235280),
      ("user2", 1525231680),
      ("user2", 1525233000),
      ("user2", 1525236480),
      ("user2", 1525237680),
      ("user3", 1525238880)
    ))
      .toDF("users", "epoch_seconds")
      .withColumn("user_session_id", SessionUDWFS.calculateSession(col("epoch_seconds"), lit(null).cast(StringType)).over(lagWindow))
      .groupBy("users", "user_session_id")
      .count
      .orderBy(col("users").asc)
      .select("count")
      .rdd
      .map(r => r(0))
      .collect()

    val actual = Array(1, 1, 2, 2, 1)

    // Test that the SUIDs are populated correctly
    assert(test.deep == actual.deep)
  }

  test("UDWF for sessionization with intial user_session_ids") {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val lagWindow = Window.partitionBy("users").orderBy("epoch_seconds")

    val test = sc.parallelize(List[(String, Long, String)](
      ("user1", 1525231800, "session_user1"),
      ("user1", 1525235280, null),
      ("user2", 1525231680, "session_user2"),
      ("user2", 1525233000, null),
      ("user2", 1525236480, null),
      ("user2", 1525237680, null),
      ("user3", 1525238880, null)
    ))
      .toDF("users", "epoch_seconds", "user_session_id")
      .withColumn("user_session_id", SessionUDWFS.calculateSession(col("epoch_seconds"), col("user_session_id")).over(lagWindow))

    val testCountArray = test.groupBy("users", "user_session_id")
      .count
      .orderBy(col("users").asc)
      .select("count")
      .rdd
      .map(r => r(0))
      .collect()

    val actualCountArray = Array(1, 1, 2, 2, 1)

    // Test that the SUIDs are populated correctly
    assert(testCountArray.deep == actualCountArray.deep)

    val testSUID = test.select("user_session_id")
      .rdd
      .map(r => r(0))
      .collect()

    // Test that the old SUID is carried over.
    assert(testSUID(3) == "session_user2")
  }
}
