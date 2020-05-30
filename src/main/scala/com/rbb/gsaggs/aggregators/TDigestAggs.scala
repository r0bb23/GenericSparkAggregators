package com.rbb.gsaggs.aggregators

import com.rbb.gsaggs.CaseClasses.HistogramAndPercentiles
import com.rbb.gsaggs.Exceptions.NotValidValue
import com.rbb.gsaggs.SparkDataFrameHelpers.getNestedRowValue
import com.tdunning.math.stats.{
  MergingDigest,
  TDigest,
}
import com.rbb.gsaggs.udfs.TDigestUDFS
import java.nio.ByteBuffer
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.types._
import org.apache.spark.sql.{
  Encoder,
  Encoders,
  Row,
}
import scala.collection.JavaConversions._

object TDigestAggs {
  case class toTDigest(
      colName: String,
      nSteps:  Option[Long] = None,
  ) extends Aggregator[
      Row,
      TDigest,
      Array[Byte]
  ] with Serializable {
    def zero: TDigest = {
      TDigest.createMergingDigest(100)
    }

    def reduce(
        tDigest: TDigest,
        row:     Row,
    ): TDigest = {
      val value = getNestedRowValue[Number](row, colName).getOrElse(-99.00)
      tDigest.add(value.asInstanceOf[Number].doubleValue)
      tDigest
    }

    def merge(
        tDigest1: TDigest,
        tDigest2: TDigest,
    ): TDigest = {
      tDigest1.add(tDigest2)
      tDigest1
    }

    def finish(
        tDigest: TDigest,
    ): Array[Byte] = {
      if (nSteps.isEmpty || tDigest.size == nSteps.get) {
        // no-op
      } else if (tDigest.size < nSteps.get) {
        val missingSteps = nSteps.get - tDigest.size
        for (i <- 1 to missingSteps.toInt) {
          tDigest.add(0.0)
        }
      } else {
        throw new NotValidValue(s"Your nSteps (${nSteps.get}) is smaller than your field count (${tDigest.size}). Check your query logic and nSteps choice.")
      }

      val buffer = ByteBuffer.allocate(tDigest.smallByteSize())
      tDigest.asSmallBytes(buffer)
      buffer.array()
    }

    def bufferEncoder: Encoder[TDigest] = Encoders.kryo[TDigest]

    def outputEncoder: Encoder[Array[Byte]] = Encoders.BINARY
  }

  case class toPercentiles(
      colName: String,
      nSteps:  Option[Long] = None,
  ) extends Aggregator[Row, TDigest, HistogramAndPercentiles] with Serializable {
    def zero: TDigest = {
      TDigest.createMergingDigest(100)
    }

    def reduce(
        tDigest: TDigest,
        row:     Row,
    ): TDigest = {
      val value = getNestedRowValue[Number](row, colName).getOrElse(-99.00)
      tDigest.add(value.asInstanceOf[Number].doubleValue)
      tDigest
    }

    def merge(
        tDigest1: TDigest,
        tDigest2: TDigest,
    ): TDigest = {
      tDigest1.add(tDigest2)
      tDigest1
    }

    def finish(
        tDigest: TDigest,
    ): HistogramAndPercentiles = {
      if (nSteps.isEmpty || tDigest.size == nSteps.get) {
        // no-op
      } else if (tDigest.size < nSteps.get) {
        val missingSteps = nSteps.get - tDigest.size
        for (i <- 1 to missingSteps.toInt) {
          tDigest.add(0.0)
        }
      } else {
        throw new NotValidValue(s"Your nSteps (${nSteps.get}) is smaller than your field count (${tDigest.size}). Check your query logic and nSteps choice.")
      }

      TDigestUDFS.getHistogramAndPercentiles(tDigest)
    }

    def bufferEncoder: Encoder[TDigest] = Encoders.kryo[TDigest]

    def outputEncoder: Encoder[HistogramAndPercentiles] = ExpressionEncoder()

  }

  case class mergeTDigests(
      colName: String,
  ) extends Aggregator[Row, TDigest, Array[Byte]] with Serializable {
    def zero: TDigest = {
      TDigest.createMergingDigest(100)
    }

    def reduce(
        tDigestCurrent: TDigest,
        row:            Row,
    ): TDigest = {
      val tDigestArray = getNestedRowValue[Array[Byte]](row, colName)
      if (tDigestArray.isDefined) {
        val buffer = ByteBuffer.allocate(tDigestArray.get.length)
        buffer.put(ByteBuffer.wrap(tDigestArray.get))
        buffer.rewind
        tDigestCurrent.add(MergingDigest.fromBytes(buffer))
      }
      tDigestCurrent
    }

    def merge(
        tDigest1: TDigest,
        tDigest2: TDigest,
    ): TDigest = {
      tDigest1.add(tDigest2)
      tDigest1
    }

    def finish(
        tDigest: TDigest,
    ): Array[Byte] = {
      val buffer = ByteBuffer.allocate(tDigest.smallByteSize())
      tDigest.asSmallBytes(buffer)
      buffer.array()
    }

    def bufferEncoder: Encoder[TDigest] = Encoders.kryo[TDigest]

    def outputEncoder: Encoder[Array[Byte]] = Encoders.BINARY
  }
}