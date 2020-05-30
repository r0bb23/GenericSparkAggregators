package com.rbb.gsaggs.aggregators

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.rbb.gsaggs.TestHelpers
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.scalatest.FunSuite

case class User(
  user:    String,
  cat_col: String
)

class DistinctSketchAggsTest extends FunSuite with DataFrameSuiteBase {
  test("To HLL Sketch Aggs test") {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val toHLL = new DistinctSketchAggs.ToHLL[String]("cat_col").toColumn.name("hll")

    val test = List(
      User("user1", "a"),
      User("user1", "b"),
      User("user1", "b"),
      User("user2", "a"),
      User("user3", "b"),
      User("user3", "c"),
    ).toDS
      .groupBy("user")
      .agg(toHLL)
      .orderBy(col("user").asc)

    val actual = sc.parallelize(List[(String, Array[Byte])](
      ("user1", TestHelpers.toHLL(List("a", "b", "b"))),
      ("user2", TestHelpers.toHLL(List("a"))),
      ("user3", TestHelpers.toHLL(List("b", "c"))),
    )).toDF("user", "hll")
      .orderBy(col("user").asc)

    assertDataFrameEquals(test, actual)
  }

  test("Merge HLL sketch test") {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val toHLL = new DistinctSketchAggs.ToHLL[String]("cat_col").toColumn.name("hll")
    val mergeHLLs = new DistinctSketchAggs.MergeHLLs("hll").toColumn.name("hll")

    val test = List(
      User("user1", "a"),
      User("user1", "b"),
      User("user1", "b"),
      User("user2", "a"),
      User("user3", "b"),
      User("user3", "c"),
    ).toDS
      .groupBy("user")
      .agg(toHLL)
      .agg(mergeHLLs)

    val actual = sc.parallelize(List[Array[Byte]](
      TestHelpers.toHLL(List("a", "b", "b", "a", "b", "c"))
    )).toDF("hll")

    assertDataFrameEquals(test, actual)
  }
}
