package com.rbb.gsaggs.udafs

import org.apache.datasketches.memory.Memory
import org.apache.datasketches.ArrayOfStringsSerDe
import org.apache.datasketches.frequencies.ItemsSketch
import org.apache.spark.sql.expressions.{
  MutableAggregationBuffer,
  UserDefinedAggregateFunction,
}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import scala.collection.JavaConversions._

object FreqSketchUDAFS {
  /*
  * https://datasketches.github.io/docs/FrequentItems/FrequentItemsErrorTable.html
  *
  * The map size is what defines how many uniques you can have before you switch
  * over to approximate counts and your error rate when you cross that threshold
  * of uniques. The value 512 is largely an arbitrary choice that seemed small
  * enough to avoid memory issues but large enough to avoid too high an error
  * rate for most of our data types.
  */
  val defaultMapSize = 512

  class toFreq(mapSize: Int = defaultMapSize) extends UserDefinedAggregateFunction {
    override def inputSchema: StructType =
      StructType(StructField("value", StringType) :: Nil)

    override def bufferSchema: StructType =
      StructType(StructField("freq_items", BinaryType) :: Nil)

    override def dataType: DataType = BinaryType

    override def deterministic: Boolean = false

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      val emptyFreq = new ItemsSketch[String](mapSize)
      buffer(0) = emptyFreq.toByteArray(new ArrayOfStringsSerDe())
    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      val freq = ItemsSketch.getInstance(Memory.wrap(buffer.getAs[Array[Byte]](0)), new ArrayOfStringsSerDe());
      freq.update(input.getAs[String](0))
      buffer(0) = freq.toByteArray(new ArrayOfStringsSerDe())
    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      val freq1 = ItemsSketch.getInstance(Memory.wrap(buffer1.getAs[Array[Byte]](0)), new ArrayOfStringsSerDe());
      val freq2 = ItemsSketch.getInstance(Memory.wrap(buffer2.getAs[Array[Byte]](0)), new ArrayOfStringsSerDe());
      freq1.merge(freq2)
      buffer1(0) = freq1.toByteArray(new ArrayOfStringsSerDe())
    }

    override def evaluate(buffer: Row): Array[Byte] = {
      val freq = ItemsSketch.getInstance(Memory.wrap(buffer.getAs[Array[Byte]](0)), new ArrayOfStringsSerDe());
      freq.toByteArray(new ArrayOfStringsSerDe())
    }
  }
}