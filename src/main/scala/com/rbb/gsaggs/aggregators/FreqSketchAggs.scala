package com.rbb.gsaggs.aggregators

import com.rbb.gsaggs.CaseClasses.FreqSketch
import com.rbb.gsaggs.SparkDataFrameHelpers.getNestedRowValue
import com.rbb.gsaggs.udfs.FreqSketchUDFS
import org.apache.datasketches.memory.Memory
import org.apache.datasketches.ArrayOfStringsSerDe
import org.apache.datasketches.frequencies.ItemsSketch
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.types._
import org.apache.spark.sql.{
  Encoder,
  Encoders,
  Row
}
import scala.collection.JavaConversions._

object FreqSketchAggs extends Serializable {
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

  case class toFreq(
      colNames: List[String],
      flatten:  Boolean = false,
      mapSize:  Int = defaultMapSize,
  ) extends Aggregator[Row, ItemsSketch[String], Array[Byte]] with Serializable {
    def zero: ItemsSketch[String] = {
      new ItemsSketch[String](mapSize)
    }

    def reduce(
        freq: ItemsSketch[String],
        row:  Row,
    ): ItemsSketch[String] = {
      if (!flatten) {
        val key = colNames.map(colName => getNestedRowValue[Any](row, colName).getOrElse("NaN").toString).mkString("_")
        freq.update(key)
      } else {
        getNestedRowValue[Seq[Any]](row, colNames(0)).getOrElse(Seq("NaN")).map {
          key =>
            freq.update(key.toString)
        }
      }
      freq
    }

    def merge(
        freq1: ItemsSketch[String],
        freq2: ItemsSketch[String],
    ): ItemsSketch[String] = {
      freq1.merge(freq2)
      freq1
    }

    def finish(
        freq: ItemsSketch[String],
    ): Array[Byte] = {
      freq.toByteArray(new ArrayOfStringsSerDe())
    }

    def bufferEncoder: Encoder[ItemsSketch[String]] = Encoders.kryo[ItemsSketch[String]]

    def outputEncoder: Encoder[Array[Byte]] = Encoders.BINARY
  }

  case class mergeFreqs(
      colName: String,
      mapSize: Int = defaultMapSize,
  ) extends Aggregator[Row, ItemsSketch[String], Array[Byte]] with Serializable {
    def zero: ItemsSketch[String] = {
      new ItemsSketch[String](mapSize)
    }

    def reduce(
        freqCurrent: ItemsSketch[String],
        row:         Row,
    ): ItemsSketch[String] = {
      val freqArray = getNestedRowValue[Array[Byte]](row, colName)
      if (freqArray.isDefined) {
        val freqOld = ItemsSketch.getInstance(Memory.wrap(freqArray.get), new ArrayOfStringsSerDe())
        freqCurrent.merge(freqOld)
      }
      freqCurrent
    }

    def merge(
        freq1: ItemsSketch[String],
        freq2: ItemsSketch[String],
    ): ItemsSketch[String] = {
      freq1.merge(freq2)
      freq1
    }

    def finish(
        freq: ItemsSketch[String],
    ): Array[Byte] = {
      freq.toByteArray(new ArrayOfStringsSerDe())
    }

    def bufferEncoder: Encoder[ItemsSketch[String]] = { Encoders.kryo[ItemsSketch[String]] }

    def outputEncoder: Encoder[Array[Byte]] = { Encoders.BINARY }
  }

  case class mergeFreqsToClass(
      colName: String,
      mapSize: Int = defaultMapSize,
  ) extends Aggregator[Row, ItemsSketch[String], List[FreqSketch]] with Serializable {
    def zero: ItemsSketch[String] = {
      new ItemsSketch[String](mapSize)
    }

    def reduce(
        freqCurrent: ItemsSketch[String],
        row:         Row,
    ): ItemsSketch[String] = {
      val freqArray = getNestedRowValue[Array[Byte]](row, colName)
      if (freqArray.isDefined) {
        val freqOld = ItemsSketch.getInstance(Memory.wrap(freqArray.get), new ArrayOfStringsSerDe())
        freqCurrent.merge(freqOld)
      }
      freqCurrent
    }

    def merge(
        freq1: ItemsSketch[String],
        freq2: ItemsSketch[String],
    ): ItemsSketch[String] = {
      freq1.merge(freq2)
      freq1
    }

    def finish(
        freq: ItemsSketch[String],
    ): List[FreqSketch] = {
      FreqSketchUDFS.freqSketchToFreqSketch(freq)
    }

    def bufferEncoder: Encoder[ItemsSketch[String]] = Encoders.kryo[ItemsSketch[String]]

    def outputEncoder: Encoder[List[FreqSketch]] = ExpressionEncoder()
  }
}