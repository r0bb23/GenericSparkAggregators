package com.rbb.gsaggs.aggregators

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.rbb.gsaggs.TestHelpers
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.scalatest.FunSuite

class DistinctSketchAggsTest extends FunSuite with DataFrameSuiteBase {
  test("To HLL Sketch Aggs test") {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

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
      .agg(DistinctSketchAggs.toHLL("cat_col").toColumn.name("hll"))
      .orderBy(col("users").asc)

    val actual = sc.parallelize(List[(String, Array[Byte])](
      ("user1", TestHelpers.toHLL(List("a", "b", "b"))),
      ("user2", TestHelpers.toHLL(List("a"))),
      ("user3", TestHelpers.toHLL(List("b", "c"))),
    )).toDF("users", "hll")
      .orderBy(col("users").asc)

    assertDataFrameEquals(test, actual)
  }

  test("Merge HLL sketch test") {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

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
      .agg(DistinctSketchAggs.toHLL("cat_col").toColumn.name("hll"))
      .groupBy()
      .agg(DistinctSketchAggs.mergeHLLs("hll").toColumn.name("hll"))

    val actual = sc.parallelize(List[Array[Byte]](
      TestHelpers.toHLL(List("a", "b", "b", "a", "b", "c"))
    )).toDF("hll")

    assertDataFrameEquals(test, actual)
  }
}
