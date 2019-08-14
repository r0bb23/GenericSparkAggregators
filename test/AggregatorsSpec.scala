package com.ionic.helperfunctions

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.ionic.helperfunctions.CaseClasses.HistogramAndPercentiles
import com.ionic.report.analyses.helpers.CaseClasses._
import com.mozilla.spark.sql.hyperloglog.aggregates._
import com.mozilla.spark.sql.hyperloglog.functions._
import org.apache.spark.SparkException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.scalatest.FunSuite

class AggregatorTests extends FunSuite with DataFrameSuiteBase {
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
      ("user1", Udfs.createTDigest(List(1.0, 2.0))),
      ("user2", Udfs.createTDigest(List(2.0, 3.0, 4.0))),
      ("user3", Udfs.createTDigest(List(3.0, 4.0)))
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
      ("user1", Udfs.createTDigest(List(1.0, 2.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0
        ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0
        ,0.0 ,0.0))),
      ("user2", Udfs.createTDigest(List(2.0, 3.0, 4.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0
        ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0
        ,0.0 ,0.0))),
      ("user3", Udfs.createTDigest(List(3.0, 4.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0
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
      ("user1", Udfs.toHistogramAndPercentiles(Udfs.createTDigest(List(1.0, 2.0)))),
      ("user2", Udfs.toHistogramAndPercentiles(Udfs.createTDigest(List(2.0, 3.0, 4.0)))),
      ("user3", Udfs.toHistogramAndPercentiles(Udfs.createTDigest(List(3.0, 4.0))))
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
      ("user1", Udfs.toHistogramAndPercentiles(Udfs.createTDigest(List(1.0, 2.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0
        ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0
        ,0.0 ,0.0)))),
      ("user2", Udfs.toHistogramAndPercentiles(Udfs.createTDigest(List(2.0, 3.0, 4.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0
        ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0
        ,0.0 ,0.0)))),
      ("user3", Udfs.toHistogramAndPercentiles(Udfs.createTDigest(List(3.0, 4.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0 ,0.0
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
      Udfs.createTDigest(List(1.0, 2.0, 2.0, 3.0, 4.0, 3.0, 4.0))
    )).toDF("tdigest")

    assertDataFrameEquals(test, actual) // equal
  }
  
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
      .agg(StreamingStatsAggs.stddevStats("double_col").toColumn.name("tsstats"))
      .select("tsstats.*")
      .first
      .toSeq

    val actual = List(0.40994110335255873, 7, 4.0, 2.7142857142857144, 1.0, 1.1126972805283737, 19.0)

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
      .agg(StreamingStatsAggs.stddevStats("double_col", Some(24)).toColumn.name("tsstats"))
      .select("tsstats.*")
      .first
      .toSeq

    val actual = List(1.7462818159328104, 24, 4.0, 0.7916666666666666, 0.0, 1.3824731042801415, 19.0)

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
        .agg(StreamingStatsAggs.stddevStats("double_col", Some(5)).toColumn.name("tsstats"))
        .select("tsstats.*")
        .first
        .toSeq
    }
  }

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
      .withColumn("user_session_id", UDWFS.calculateSession(col("epoch_seconds"), lit(null).cast(StringType)).over(lagWindow))
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
      .withColumn("user_session_id", UDWFS.calculateSession(col("epoch_seconds"), col("user_session_id")).over(lagWindow))
      
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
