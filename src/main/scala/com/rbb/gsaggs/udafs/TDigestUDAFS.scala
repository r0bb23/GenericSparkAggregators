package com.rbb.gsaggs.udafs

import com.rbb.gsaggs.ScalaHelpers.{
  byteArrayToObject,
  objectToByteArray,
}
import com.tdunning.math.stats.{
  MergingDigest,
  TDigest,
}
import java.nio.ByteBuffer
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{
  MutableAggregationBuffer,
  UserDefinedAggregateFunction,
}
import org.apache.spark.sql.types.{
  BinaryType,
  DataType,
  DoubleType,
  StructField,
  StructType,
}
import scala.collection.JavaConversions._
import scala.math

object TDigestUDAFS {
  class toTDigest extends UserDefinedAggregateFunction {
    // This is the input fields for your aggregate function.
    override def inputSchema: StructType =
      StructType(StructField("value", DoubleType) :: Nil)

    // This is the internal fields you keep for computing your aggregate.
    override def bufferSchema: StructType =
      StructType(StructField("tdigest", BinaryType) :: Nil)

    // This is the output type of your aggregatation function.
    override def dataType: DataType = BinaryType

    override def deterministic: Boolean = false

    // This is the initial value for your buffer schema.
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = objectToByteArray[TDigest](TDigest.createMergingDigest(100))
    }

    // This is how to update your buffer schema given an input.
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      val tdigest = byteArrayToObject[TDigest](buffer.getAs[Array[Byte]](0))
      tdigest.add(input.getAs[Double](0))
      buffer(0) = objectToByteArray[TDigest](tdigest)
    }

    // This is how to merge two objects with the bufferSchema type.
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      val tdigest1 = byteArrayToObject[TDigest](buffer1.getAs[Array[Byte]](0))
      val tdigest2 = byteArrayToObject[TDigest](buffer2.getAs[Array[Byte]](0))
      tdigest1.add(tdigest2)
      buffer1(0) = objectToByteArray[TDigest](tdigest1)
    }

    // This is where you output the final value, given the final value of your bufferSchema.
    override def evaluate(buffer: Row): Array[Byte] = {
      val tdigest = byteArrayToObject[TDigest](buffer.getAs[Array[Byte]](0))
      val tdigestBuffer = ByteBuffer.allocate(tdigest.smallByteSize())
      tdigest.asSmallBytes(tdigestBuffer)
      tdigestBuffer.array()
    }
  }
}