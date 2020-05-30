package com.rbb.gsaggs.udfs

import com.rbb.gsaggs.CaseClasses.FreqSketch
import org.apache.datasketches.ArrayOfStringsSerDe
import org.apache.datasketches.frequencies.ErrorType;
import org.apache.datasketches.frequencies.ItemsSketch
import org.apache.datasketches.memory.Memory
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.udf
import scala.collection.JavaConversions._
import scala.math

object FreqSketchUDFS {
  // Decides what type of error to NOT favor when estimating value counts.
  val errorType = ErrorType.NO_FALSE_NEGATIVES

  def freqEuclidDist(
      freqModelBinary: Array[Byte],
      freqTestBinary:  Array[Byte],
  ): Double = {
    def calcEuclidDist(
        freqMerged: ItemsSketch[String],
        freqModel:  ItemsSketch[String],
        freqTest:   ItemsSketch[String],
    ): Double = {
      val freqModelItems = freqModel.getFrequentItems(errorType)
      val freqTestItems = freqTest.getFrequentItems(errorType)
      val freqMergedItems = freqMerged.getFrequentItems(errorType)
      val freqModelEstimatesSum = freqModelItems.foldLeft(0L) { (sum, row) => sum + row.getEstimate }.toDouble
      val freqTestEstimatesSum = freqTestItems.foldLeft(0L) { (sum, row) => sum + row.getEstimate }.toDouble

      var freqSquaredSum = 0.0
      freqMergedItems.foreach{
        row => {
          val modelRowValue = freqModel.getEstimate(row.getItem) / freqModelEstimatesSum
          val testRowValue = freqTest.getEstimate(row.getItem) / freqTestEstimatesSum
          freqSquaredSum += math.pow(modelRowValue - testRowValue, 2)
        }
      }

      math.pow(freqSquaredSum, 0.5)
    }

    val freqModel = ItemsSketch.getInstance(Memory.wrap(freqModelBinary), new ArrayOfStringsSerDe())
    val freqTest = ItemsSketch.getInstance(Memory.wrap(freqTestBinary), new ArrayOfStringsSerDe())

    val freqMerged = new ItemsSketch[String](math.max(freqModel.getMaximumMapCapacity * 4 / 3, freqTest.getMaximumMapCapacity * 4 / 3))
    freqMerged.merge(freqModel).merge(freqTest)

    calcEuclidDist(freqMerged, freqModel, freqTest)
  }

  val freqEuclidDistUdf = udf((freqModelBinary: Array[Byte], freqTestBinary: Array[Byte]) => freqEuclidDist(freqModelBinary, freqTestBinary))

  def freqSketchToFreqSketch(
      freq: ItemsSketch[String],
  ): List[FreqSketch] = {
    val freqItems = freq.getFrequentItems(errorType)
    val freqEstimatesSum = freqItems.foldLeft(0L) {
      (sum, row) => sum + row.getEstimate
    }.toDouble

    val freqs = freqItems.map {
      row =>
        FreqSketch(
          value   = row.getItem,
          freq    = row.getEstimate,
          freq_05 = row.getLowerBound,
          freq_95 = row.getUpperBound,
          percent = row.getEstimate / freqEstimatesSum
        )
    }

    freqs.toList
  }

  def freqSketchArrayToFreqSketch(
      freqArray: Array[Byte],
  ): List[FreqSketch] = {
    val freq = ItemsSketch.getInstance(Memory.wrap(freqArray), new ArrayOfStringsSerDe())
    freqSketchToFreqSketch(freq)
  }

  val freqSketchArrayToFreqSketchUdf = udf((freqArray: Array[Byte]) => freqSketchArrayToFreqSketch(freqArray))
}