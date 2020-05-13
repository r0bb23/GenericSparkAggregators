package com.rbb.gsaggs.udafs

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.rbb.gsaggs.udfs.TDigestUDFS
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.scalatest.FunSuite

class TDigestUDAFSTest extends FunSuite with DataFrameSuiteBase {
  import spark.implicits._

  test("To tdigest UDAF test") {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val tDigest = new TDigestUDAFS.toTDigest

    val test = sc.parallelize(List[(String, Double)](
      ("user1", 1.0),
      ("user1", 2.0),
      ("user2", 2.0),
      ("user2", 3.0),
      ("user2", 4.0),
      ("user3", 3.0),
      ("user3", 4.0),
    ))
      .toDF("users", "double_col")
      .groupBy("users")
      .agg(tDigest(col("double_col")).as("tdigest"))
      .orderBy(col("users").asc)

    val actual = sc.parallelize(List[(String, Array[Byte])](
      ("user1", TDigestUDFS.createTDigest(List(1.0, 2.0))),
      ("user2", TDigestUDFS.createTDigest(List(2.0, 3.0, 4.0))),
      ("user3", TDigestUDFS.createTDigest(List(3.0, 4.0))),
    )).toDF("users", "tdigest")
      .orderBy(col("users").asc)

    assertDataFrameEquals(test, actual) // equal
  }
}
