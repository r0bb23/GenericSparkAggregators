package com.rbb.gsaggs.udfs

import com.rbb.gsaggs.CaseClasses.{
  IntermediateStddevStats,
  StddevStats
}
import scala.collection.JavaConversions._
import scala.math

object StreamingStatsUDFS {
  def mergeStddevStats(
      stddevStats1: IntermediateStddevStats,
      stddevStats2: IntermediateStddevStats,
  ): IntermediateStddevStats = {
    val newCount = stddevStats1.count + stddevStats2.count
    val newSum = stddevStats1.sum + stddevStats2.sum
    val delta = stddevStats2.mean - stddevStats1.mean
    val (newMean, newM2) = if (delta != 0 & newCount != 0) {
    val newMean = (stddevStats1.mean * stddevStats1.count + stddevStats2.mean * stddevStats2.count) / newCount
    val newM2 = stddevStats1.m2 + stddevStats2.m2 + math.pow(delta, 2.0) * stddevStats1.count * stddevStats2.count / newCount

    (newMean, newM2)
    } else {
    (stddevStats1.mean, stddevStats1.m2 + stddevStats2.m2)
    }
    val newMin = (stddevStats1.min, stddevStats2.min) match {
    case (None, None) => None
    case (Some(min1), None) => Some(min1)
    case (None, Some(min2)) => Some(min2)
    case (Some(min1), Some(min2)) => Some(math.min(min1, min2))
    }

    IntermediateStddevStats(
    count = newCount,
    m2    = newM2,
    max   = math.max(stddevStats1.max, stddevStats2.max),
    mean  = newMean,
    min   = newMin,
    sum   = newSum
    )
  }

  def stddevStatsClass(
      stats: IntermediateStddevStats,
  ): StddevStats = {
    val stddev = math.sqrt(stats.m2 / (stats.count - 1.0))

      StddevStats(
        coefvar = stddev / stats.mean,
        count   = stats.count,
        max     = stats.max,
        mean    = stats.mean,
        min     = stats.min.get,
        stddev  = stddev,
        sum     = stats.sum
      )
  }
}