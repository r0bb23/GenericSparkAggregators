package com.rbb.gsaggs.aggregators

import com.rbb.gsaggs.CaseClasses.{
  IntermediateStddevStats,
  StddevStats,
}
import com.rbb.gsaggs.Exceptions.NotValidValue
import com.rbb.gsaggs.SparkDataFrameHelpers.getNestedRowValue
import com.rbb.gsaggs.udfs.Udfs.{
    mergeStddevStats,
    stddevStatsClass,
}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.types._
import org.apache.spark.sql.{
    Encoder,
    Row,
}
import scala.collection.JavaConversions._
import scala.math.{
    max,
    min,
    pow,
    sqrt,
}

object StreamingStatsAggs {
  // Assumes count will always be > 1.
  // Also assumes min value of 0.0 when using nStep option.
  // In general, if either of those things breaks using this agg you should check to make sure something isn't wrong else where first.
  case class toStddevStats(colName: String, nSteps: Option[Long] = None) extends Aggregator[Row, IntermediateStddevStats, IntermediateStddevStats] with Serializable {
    def zero: IntermediateStddevStats = {
      IntermediateStddevStats(
        count = 0,
        m2    = 0.0,
        max   = 0.0,
        mean  = 0.0,
        min   = None,
        sum   = 0.0
      )
    }

    // Uses the following algorithm https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Online_algorithm
    def reduce(currentStddevStats: IntermediateStddevStats, row: Row): IntermediateStddevStats = {
      val newValue = getNestedRowValue[Number](row, colName)
        .getOrElse(0.0)
        .asInstanceOf[Number]
        .doubleValue
      val newCount = currentStddevStats.count + 1
      val newSum = currentStddevStats.sum + newValue
      val delta = newValue - currentStddevStats.mean
      val newMean = currentStddevStats.mean + delta / newCount
      val delta2 = newValue - newMean
      val newM2 = currentStddevStats.m2 + delta * delta2
      val newMin = if (!currentStddevStats.min.isEmpty) {
        min(currentStddevStats.min.get, newValue)
      } else {
        newValue
      }

      IntermediateStddevStats(
        count = newCount,
        m2    = newM2,
        max   = max(currentStddevStats.max, newValue),
        mean  = newMean,
        min   = Some(newMin),
        sum   = newSum
      )
    }

    // Uses the following algorithm: https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Parallel_algorithm
    def merge(stddevStats1: IntermediateStddevStats, stddevStats2: IntermediateStddevStats): IntermediateStddevStats = {
      mergeStddevStats(stddevStats1, stddevStats2)
    }

    def finish(stddevStats: IntermediateStddevStats): IntermediateStddevStats = {
      if (nSteps.isEmpty) {
        stddevStats
      } else {
        val newMin = if (stddevStats.count < nSteps.get) {
          0.0
        } else if (stddevStats.count == nSteps.get) {
          stddevStats.min.get
        } else {
          throw new NotValidValue(s"Your nSteps (${nSteps.get}) is smaller than your field count (${stddevStats.count}). Check your query logic and nSteps choice.")
        }
        val missingSteps = nSteps.get - stddevStats.count
        val delta = 0.0 - stddevStats.mean
        val newMean = stddevStats.mean * stddevStats.count / nSteps.get

        val newM2 = if (missingSteps > 0) {
          stddevStats.m2 + 0.0 + pow(delta, 2.0) * stddevStats.count * missingSteps / nSteps.get
        } else {
          stddevStats.m2
        }

        IntermediateStddevStats(
          count = nSteps.get,
          m2    = newM2,
          max   = stddevStats.max,
          mean  = newMean,
          min   = Some(newMin),
          sum   = stddevStats.sum
        )
      }
    }

    def bufferEncoder: Encoder[IntermediateStddevStats] = ExpressionEncoder()

    def outputEncoder: Encoder[IntermediateStddevStats] = ExpressionEncoder()
  }
  
  // Assumes count will always be > 1.
  // Also assumes min value of 0.0 when using nStep option.
  // In general, if either of those things breaks using this agg you should check to make sure something isn't wrong else where first.
  case class mergeStddevStats(colName: String, nSteps: Option[Long] = None) extends Aggregator[Row, IntermediateStddevStats, IntermediateStddevStats] with Serializable {
    def zero: IntermediateStddevStats = {
      IntermediateStddevStats(
        count = 0,
        m2    = 0.0,
        max   = 0.0,
        mean  = 0.0,
        min   = None,
        sum   = 0.0
      )
    }

    // Uses the following algorithm https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Online_algorithm
    def reduce(currentStddevStats: IntermediateStddevStats, row: Row): IntermediateStddevStats = {
      val oldStddevStats = getNestedRowValue[IntermediateStddevStats](row, colName)
        .getOrElse(None)
        .asInstanceOf[IntermediateStddevStats]
      
      if oldStddevStats {
        mergeStddevStats(currentStddevStats, stddevStats2)
      } else {
        currentStddevStats
      }
    }

    // Uses the following algorithm: https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Parallel_algorithm
    def merge(stddevStats1: IntermediateStddevStats, stddevStats2: IntermediateStddevStats): IntermediateStddevStats = {
      mergeStddevStats(stddevStats1, stddevStats2)
    }

    def finish(stddevStats: IntermediateStddevStats): IntermediateStddevStats = {
      if (nSteps.isEmpty) {
        stddevStats
      } else {
        val newMin = if (stddevStats.count < nSteps.get) {
          0.0
        } else if (stddevStats.count == nSteps.get) {
          stddevStats.min.get
        } else {
          throw new NotValidValue(s"Your nSteps (${nSteps.get}) is smaller than your field count (${stddevStats.count}). Check your query logic and nSteps choice.")
        }
        val missingSteps = nSteps.get - stddevStats.count
        val delta = 0.0 - stddevStats.mean
        val newMean = stddevStats.mean * stddevStats.count / nSteps.get

        val newM2 = if (missingSteps > 0) {
          stddevStats.m2 + 0.0 + pow(delta, 2.0) * stddevStats.count * missingSteps / nSteps.get
        } else {
          stddevStats.m2
        }

        IntermediateStddevStats(
          count = nSteps.get,
          m2    = newM2,
          max   = stddevStats.max,
          mean  = newMean,
          min   = Some(newMin),
          sum   = stddevStats.sum
        )
      }
    }

    def bufferEncoder: Encoder[IntermediateStddevStats] = ExpressionEncoder()

    def outputEncoder: Encoder[IntermediateStddevStats] = ExpressionEncoder()
  }
  
  // Assumes count will always be > 1.
  // Also assumes min value of 0.0 when using nStep option.
  // In general, if either of those things breaks using this agg you should check to make sure something isn't wrong else where first.
  case class mergeStddevStatsToClass(colName: String, nSteps: Option[Long] = None) extends Aggregator[Row, IntermediateStddevStats, StddevStats] with Serializable {
    def zero: IntermediateStddevStats = {
      IntermediateStddevStats(
        count = 0,
        m2    = 0.0,
        max   = 0.0,
        mean  = 0.0,
        min   = None,
        sum   = 0.0
      )
    }

    // Uses the following algorithm https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Online_algorithm
    def reduce(currentStddevStats: IntermediateStddevStats, row: Row): IntermediateStddevStats = {
      val oldStddevStats = getNestedRowValue[IntermediateStddevStats](row, colName)
        .getOrElse(None)
        .asInstanceOf[IntermediateStddevStats]
      
      if oldStddevStats {
        mergeStddevStats(currentStddevStats, stddevStats2)
      } else {
        currentStddevStats
      }
    }

    // Uses the following algorithm: https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Parallel_algorithm
    def merge(stddevStats1: IntermediateStddevStats, stddevStats2: IntermediateStddevStats): IntermediateStddevStats = {
      mergeStddevStats(stddevStats1, stddevStats2)
    }

    def finish(stddevStats: IntermediateStddevStats): StddevStats = {
      val stats = if (nSteps.isEmpty) {
        stddevStats
      } else {
        val newMin = if (stddevStats.count < nSteps.get) {
          0.0
        } else if (stddevStats.count == nSteps.get) {
          stddevStats.min.get
        } else {
          throw new NotValidValue(s"Your nSteps (${nSteps.get}) is smaller than your field count (${stddevStats.count}). Check your query logic and nSteps choice.")
        }
        val missingSteps = nSteps.get - stddevStats.count
        val delta = 0.0 - stddevStats.mean
        val newMean = stddevStats.mean * stddevStats.count / nSteps.get

        val newM2 = if (missingSteps > 0) {
          stddevStats.m2 + 0.0 + pow(delta, 2.0) * stddevStats.count * missingSteps / nSteps.get
        } else {
          stddevStats.m2
        }

        IntermediateStddevStats(
          count = nSteps.get,
          m2    = newM2,
          max   = stddevStats.max,
          mean  = newMean,
          min   = Some(newMin),
          sum   = stddevStats.sum
        )
      }

      stddevStatsClass(stats)
    }

    def bufferEncoder: Encoder[IntermediateStddevStats] = ExpressionEncoder()

    def outputEncoder: Encoder[StddevStats] = ExpressionEncoder()
  }
}