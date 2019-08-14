package com.ionic.helperfunctions

import org.apache.spark.sql.Column

object CaseClasses {
  case class ColChanges(
      oldCol:  Column,
      newCols: Seq[NewCol]
  )

  case class FreqSketch(
      value:   String,
      freq:    Long,
      freq_05: Long,
      freq_95: Long,
      percent: Double
  )

  case class Histogram(
      centroid: Double,
      count:    Int,
      sum:      Double
  )

  case class HistogramAndPercentiles(
      buckets: Array[Histogram],
      iqr:     Double,
      // Whiskers are calculated using the typical iqr *1.5 method:
      // http://mathworld.wolfram.com/Box-and-WhiskerPlot.html (first method descriped on this page.)
      lower_whisker: Double,
      median:        Double,
      percentile_05: Double,
      percentile_25: Double,
      percentile_75: Double,
      percentile_95: Double,
      upper_whisker: Double
  )

  case class HistogramAndStats(
      buckets: Array[Histogram],
      stats:   Stats
  )

  case class IntermediateStddevStats(
      count: Long,
      m2:    Double,
      max:   Double,
      mean:  Double,
      min:   Option[Double],
      sum:   Double
  )

  case class NewCol(
      name: String,
      col:  Column
  )

  case class SimScore(
      dice_sorensen_score: Double,
      geo_mean_score:      Double,
      jaro_score:          Double,
      ngram_score:         Double,
      overlap_score:       Double
  )

  case class Stats(
      count: Long,
      iqr:   Double,
      // Whiskers are calculated using the typical iqr *1.5 method:
      // http://mathworld.wolfram.com/Box-and-WhiskerPlot.html (first method descriped on this page.)
      lower_whisker: Double,
      max:           Double,
      mean:          Double,
      median:        Double,
      min:           Double,
      percentile_05: Double,
      percentile_25: Double,
      percentile_75: Double,
      percentile_95: Double,
      sum:           Double,
      upper_whisker: Double
  )

  case class StddevStats(
      coefvar: Double,
      count:   Long,
      max:     Double,
      mean:    Double,
      min:     Double,
      stddev:  Double,
      sum:     Double
  )

  case class Table(
      compression_type: String       = "snappy",
      create_table:     Boolean      = true,
      database_name:    String,
      drop_table:       Boolean      = false,
      location_name:    String,
      partition_by:     List[String],
      storage_type:     String       = "PARQUET",
      table_name:       String,
      write_mode:       String       = "append"
  )

  case class TimeseriesHistogramAndStats(
      time_interval:   Int,
      histogram_array: Array[HistogramAndStats]
  )

  case class TimeRange(
      is_whitelist: Boolean,
      time_ranges:  Set[String]
  )
}
