package com.rbb.gsaggs.aggregators

import com.rbb.gsaggs.SparkDataFrameHelpers.getNestedRowValue
import org.apache.datasketches.memory.Memory
import org.apache.datasketches.hll.{
  HllSketch,
  TgtHllType,
  Union,
}
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{
  Encoder,
  Encoders,
  Row
}
import scala.collection.JavaConversions._

object DistinctSketchAggs extends Serializable {
  val defaultLogK = 10

  case class toHLL(
      colName: String,
      logK:    Int = defaultLogK,
  ) extends Aggregator[
      Row,
      HllSketch,
      Array[Byte],
  ] with Serializable {
    def zero: HllSketch = {
      new HllSketch(logK)
    }

    def reduce(
        hll: HllSketch,
        row: Row,
    ): HllSketch = {
      val value = getNestedRowValue[String](row, colName).getOrElse(None)
      hll.update(value.asInstanceOf[String])
      hll
    }

    def merge(
        hll1: HllSketch,
        hll2: HllSketch,
    ): HllSketch = {
      val union = new Union(logK)
      union.update(hll1)
      union.update(hll2)
      union.getResult(union.getTgtHllType())
    }

    def finish(
        hll: HllSketch,
    ): Array[Byte] = {
      hll.toCompactByteArray()
    }

    def bufferEncoder: Encoder[HllSketch] = {
      Encoders.kryo[HllSketch]
    }

    def outputEncoder: Encoder[Array[Byte]] = {
      Encoders.BINARY
    }
  }

  case class mergeHLLs(
      colName: String,
      logK:    Int = defaultLogK,
  ) extends Aggregator[Row, HllSketch, Array[Byte]] with Serializable {
    def zero: HllSketch = {
      new HllSketch(logK)
    }

    def reduce(
        hllCurrent: HllSketch,
        row:        Row,
    ): HllSketch = {
      val hllArray = getNestedRowValue[Array[Byte]](row, colName)
        .getOrElse((new HllSketch(logK)).toCompactByteArray())
      val hllOld = HllSketch.heapify(Memory.wrap(hllArray))
      val union = new Union(logK)
      union.update(hllOld)
      union.update(hllCurrent)
      union.getResult(union.getTgtHllType())
    }

    def merge(
        hll1: HllSketch,
        hll2: HllSketch,
    ): HllSketch = {
      val union = new Union(logK)
      union.update(hll1)
      union.update(hll2)
      union.getResult(union.getTgtHllType())
    }

    def finish(
        hll: HllSketch,
    ): Array[Byte] = {
      hll.toCompactByteArray()
    }

    def bufferEncoder: Encoder[HllSketch] = {
      Encoders.kryo[HllSketch]
    }

    def outputEncoder: Encoder[Array[Byte]] = {
      Encoders.BINARY
    }
  }
}