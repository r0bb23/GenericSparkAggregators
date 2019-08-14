package com.ionic.helperfunctions

import com.holdenkarau.spark.testing.{ SharedSparkContext, DataFrameSuiteBase }
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class ColAnyToStrTest extends FunSuite with DataFrameSuiteBase {

  test("Sequence of integer column") {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val usercounts = Seq(
      ("5ab2bce96b3c6549830f40c1", "5ab2bce96b3c6549830f40d1", 2, Seq(1, 2)),
      ("5ab2bce96b3c6549830f40c1", "5ab2bce96b3c6549830f40d2", 3, Seq(1, 2))
    )

    val cts = usercounts
      .toDF("tenant_id", "user_id", "ct", "groups")
      .transform(DataFrameTransformers.ColAnyToStr("groups"))
      .collect

    cts.foreach(row => {
      assert(row.getString(3) == "(1, 2)")
    })
  }

  test("Sequence of string column") {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val usercounts = Seq(
      ("5ab2bce96b3c6549830f40c1", "5ab2bce96b3c6549830f40d1", 2, Seq("a", "b")),
      ("5ab2bce96b3c6549830f40c1", "5ab2bce96b3c6549830f40d2", 3, Seq("a", "b"))
    )

    val cts = usercounts
      .toDF("tenant_id", "user_id", "ct", "groups")
      .transform(DataFrameTransformers.ColAnyToStr("groups"))
      .collect

    cts.foreach(row => {
      assert(row.getString(3) == "(a, b)")
    })
  }

}
