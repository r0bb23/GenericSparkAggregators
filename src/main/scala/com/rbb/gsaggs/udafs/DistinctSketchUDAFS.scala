package com.rbb.gsaggs.udafs

import org.apache.datasketches.memory.Memory
import org.apache.datasketches.ArrayOfStringsSerDe
import org.apache.datasketches.hll.HllSketch
import org.apache.datasketches.hll.TgtHllType
import org.apache.datasketches.hll.Union
import org.apache.spark.sql.expressions.{
  MutableAggregationBuffer,
  UserDefinedAggregateFunction,
}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import scala.collection.JavaConversions._

object DistinctSketchUDAFS {
  val defaultLogK = 10

  class toHLL(
      logK: Int = defaultLogK,
  ) extends UserDefinedAggregateFunction {
    override def inputSchema: StructType =
      StructType(StructField("value", StringType) :: Nil)

    override def bufferSchema: StructType =
      StructType(StructField("freq_items", BinaryType) :: Nil)

    override def dataType: DataType = BinaryType

    override def deterministic: Boolean = false

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      val hll = new HllSketch(logK)
      buffer(0) = hll.toCompactByteArray()
    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      val hll = HllSketch.heapify(Memory.wrap(buffer.getAs[Array[Byte]](0)))
      hll.update(input.getAs[String](0))
      buffer(0) = hll.toCompactByteArray()
    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      val hll1 = HllSketch.heapify(Memory.wrap(buffer1.getAs[Array[Byte]](0)))
      val hll2 = HllSketch.heapify(Memory.wrap(buffer2.getAs[Array[Byte]](0)))
      val union = new Union(logK);
      union.update(hll1);
      union.update(hll2);
      buffer1(0) = union
        .getResult(hll1.getTgtHllType())
        .toCompactByteArray()
    }

    override def evaluate(buffer: Row): Array[Byte] = {
      val hll = HllSketch.heapify(Memory.wrap(buffer.getAs[Array[Byte]](0)))
      hll.toCompactByteArray()
    }
  }
}