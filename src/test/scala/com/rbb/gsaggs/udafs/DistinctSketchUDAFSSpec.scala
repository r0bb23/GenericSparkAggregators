package com.rbb.gsaggs.udafs

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.rbb.gsaggs.TestHelpers
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.scalatest.FunSuite

class DistinctSketchUDAFSTest extends FunSuite with DataFrameSuiteBase {
  test("To Freq Sketch UDAF test") {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val hllSketch = new DistinctSketchUDAFS.toHLL

    val test = sc.parallelize(List[(String, String)](
      ("user1", "a"),
      ("user1", "b"),
      ("user1", "b"),
      ("user2", "a"),
      ("user3", "b"),
      ("user3", "c"),
    ))
      .toDF("users", "cat_col")
      .groupBy("users")
      .agg(hllSketch(col("cat_col")).as("hll"))
      .orderBy(col("users").asc)

    val actual = sc.parallelize(List[(String, Array[Byte])](
      ("user1", TestHelpers.toHLL(List("a", "b", "b"))),
      ("user2", TestHelpers.toHLL(List("a"))),
      ("user3", TestHelpers.toHLL(List("b", "c"))),
    )).toDF("users", "hll")
      .orderBy(col("users").asc)

    assertDataFrameEquals(test, actual)
  }
}
