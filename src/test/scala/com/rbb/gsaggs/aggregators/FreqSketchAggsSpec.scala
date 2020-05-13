package com.rbb.gsaggs.aggregators

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.rbb.gsaggs.TestHelpers
import com.rbb.gsaggs.udfs._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.scalatest.FunSuite

class FreqSketchAggsTests extends FunSuite with DataFrameSuiteBase {
  test("To sketch frequency test") {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val test = sc.parallelize(List[(String, String)](
      ("user1", "a"),
      ("user1", "b"),
      ("user2", "a"),
      ("user2", "a"),
      ("user2", "c"),
      ("user3", "b"),
      ("user3", "c")
    ))
      .toDF("users", "cat_col")
      .groupBy("users")
      .agg(FreqSketchAggs.toFreq(List("cat_col")).toColumn.name("freq"))
      .orderBy(col("users").asc)

    val actual = sc.parallelize(List[(String, Array[Byte])](
      ("user1", TestHelpers.toSketchFrequency(List("a", "b"))),
      ("user2", TestHelpers.toSketchFrequency(List("a", "a", "c"))),
      ("user3", TestHelpers.toSketchFrequency(List("b", "c")))
    )).toDF("users", "freq")
      .orderBy(col("users").asc)

    assertDataFrameEquals(test, actual) // equal
  }

  test("Merge sketch frequency test") {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val test = sc.parallelize(List[(String, String)](
      ("user1", "a"),
      ("user1", "b"),
      ("user2", "a"),
      ("user2", "a"),
      ("user2", "c"),
      ("user3", "b"),
      ("user3", "c")
    ))
      .toDF("users", "cat_col")
      .groupBy("users")
      .agg(FreqSketchAggs.toFreq(List("cat_col")).toColumn.name("freq"))
      .groupBy()
      .agg(FreqSketchAggs.mergeFreqs("freq").toColumn.name("freq"))

    val actual = sc.parallelize(List[Array[Byte]](
      TestHelpers.toSketchFrequency(List("a", "b", "a", "a", "c", "b", "c"))
    )).toDF("freq")

    assertDataFrameEquals(test, actual) // equal
  }
}