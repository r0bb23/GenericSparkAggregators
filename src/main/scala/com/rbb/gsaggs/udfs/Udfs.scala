package com.rbb.gsaggs.udfs

import org.apache.spark.sql.SparkSession

object UDFS {
  def registerUDFs(): SparkSession = {
    val spark = SparkSession
      .builder()
      .getOrCreate()


    // spark.udf.register("array_to_string", GeneralUDFS.arrayToString _)
    // spark.udf.register("create_tdigest", createTDigest[Double] _)
    // spark.udf.register("double", GeneralUDFS.numericToDouble _)
    // spark.udf.register("empty_tdigest", createEmptyTDigest _)
    // spark.udf.register("first_array_element", GeneralUDFS.getFirstArrayElement _)
    // spark.udf.register("freq_class", FreqSketchUdfs.freqSketchArrayToFreqSketch _)
    // spark.udf.register("get_ip_24block", GeneralUDFS.get_ip_24block _)
    // spark.udf.register("get_ip_24block_array", GeneralUDFS.get_ip_24block_array _)
    // spark.udf.register("histogram_and_percentiles", toHistogramAndPercentiles _)
    // spark.udf.register("histogram_and_stats", toHistogramAndStats _)
    // spark.udf.register("replace_empty_string_array", GeneralUDFS.replaceEmptyStringArray _)
    // spark.udf.register("replace_string", GeneralUDFS.replaceString _)
    // spark.udf.register("sim_scores", GeneralUDFS.getSimScores _)
    // spark.udf.register("tdigest", new toTDigest)

    spark
  }
}