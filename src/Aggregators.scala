package com.ionic.helperfunctions

import com.ionic.helperfunctions.CaseClasses.{ FreqSketch, HistogramAndPercentiles, IntermediateStddevStats, StddevStats }
import com.ionic.helperfunctions.Exceptions.NotValidValue
import com.ionic.helperfunctions.ScalaHelpers.{ byteArrayToObject, objectToByteArray }
import com.ionic.helperfunctions.SparkDataFrameHelpers.getNestedRowValue
import com.tdunning.math.stats.{ MergingDigest, TDigest }
import com.yahoo.memory.Memory
import com.yahoo.sketches.ArrayOfStringsSerDe
import com.yahoo.sketches.frequencies.ItemsSketch
import java.nio.ByteBuffer
import java.util.UUID
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{ Add, AggregateWindowFunction, AttributeReference, Expression, If,
  IsNotNull, LessThanOrEqual, Literal, ScalaUDF, Subtract }
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ Column, Encoder, Encoders, Row }
import org.apache.spark.unsafe.types.UTF8String
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.math.{ max, min, pow, sqrt }

object TDigestAggs {
  case class toTDigest(colName: String, nSteps: Option[Long] = None) extends Aggregator[Row, TDigest, Array[Byte]] with Serializable {
    def zero: TDigest = {
      TDigest.createMergingDigest(100)
    }

    def reduce(tDigest: TDigest, row: Row): TDigest = {
      val value = getNestedRowValue[Number](row, colName).getOrElse(-99.00)
      tDigest.add(value.asInstanceOf[Number].doubleValue)
      tDigest
    }

    def merge(tDigest1: TDigest, tDigest2: TDigest): TDigest = {
      tDigest1.add(tDigest2)
      tDigest1
    }

    def finish(tDigest: TDigest): Array[Byte] = {
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

  case class toPercentiles(colName: String, nSteps: Option[Long] = None) extends Aggregator[Row, TDigest, HistogramAndPercentiles] with Serializable {
    def zero: TDigest = {
      TDigest.createMergingDigest(100)
    }

    def reduce(tDigest: TDigest, row: Row): TDigest = {
      val value = getNestedRowValue[Number](row, colName).getOrElse(-99.00)
      tDigest.add(value.asInstanceOf[Number].doubleValue)
      tDigest
    }

    def merge(tDigest1: TDigest, tDigest2: TDigest): TDigest = {
      tDigest1.add(tDigest2)
      tDigest1
    }

    def finish(tDigest: TDigest): HistogramAndPercentiles = {
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

      Udfs.getHistogramAndPercentiles(tDigest)
    }

    def bufferEncoder: Encoder[TDigest] = Encoders.kryo[TDigest]

    def outputEncoder: Encoder[HistogramAndPercentiles] = ExpressionEncoder()

  }

  case class mergeTDigests(colName: String) extends Aggregator[Row, TDigest, Array[Byte]] with Serializable {
    def zero: TDigest = {
      TDigest.createMergingDigest(100)
    }

    def reduce(tDigestCurrent: TDigest, row: Row): TDigest = {
      val tDigestArray = getNestedRowValue[Array[Byte]](row, colName)
      if (tDigestArray.isDefined) {
        val buffer = ByteBuffer.allocate(tDigestArray.get.length)
        buffer.put(ByteBuffer.wrap(tDigestArray.get))
        buffer.rewind
        tDigestCurrent.add(MergingDigest.fromBytes(buffer))
      }
      tDigestCurrent
    }

    def merge(tDigest1: TDigest, tDigest2: TDigest): TDigest = {
      tDigest1.add(tDigest2)
      tDigest1
    }

    def finish(tDigest: TDigest): Array[Byte] = {
      val buffer = ByteBuffer.allocate(tDigest.smallByteSize())
      tDigest.asSmallBytes(buffer)
      buffer.array()
    }

    def bufferEncoder: Encoder[TDigest] = Encoders.kryo[TDigest]

    def outputEncoder: Encoder[Array[Byte]] = Encoders.BINARY
  }
}

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

  case class toFreq(colNames: List[String], flatten: Boolean = false, mapSize: Int = defaultMapSize) extends Aggregator[Row, ItemsSketch[String], Array[Byte]] with Serializable {
    def zero: ItemsSketch[String] = {
      new ItemsSketch[String](mapSize)
    }

    def reduce(freq: ItemsSketch[String], row: Row): ItemsSketch[String] = {
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

    def merge(freq1: ItemsSketch[String], freq2: ItemsSketch[String]): ItemsSketch[String] = {
      freq1.merge(freq2)
      freq1
    }

    def finish(freq: ItemsSketch[String]): Array[Byte] = {
      freq.toByteArray(new ArrayOfStringsSerDe())
    }

    def bufferEncoder: Encoder[ItemsSketch[String]] = Encoders.kryo[ItemsSketch[String]]

    def outputEncoder: Encoder[Array[Byte]] = Encoders.BINARY
  }

  case class mergeFreqs(colName: String, mapSize: Int = defaultMapSize) extends Aggregator[Row, ItemsSketch[String], Array[Byte]] with Serializable {
    def zero: ItemsSketch[String] = {
      new ItemsSketch[String](mapSize)
    }

    def reduce(freqCurrent: ItemsSketch[String], row: Row): ItemsSketch[String] = {
      val freqArray = getNestedRowValue[Array[Byte]](row, colName)
      if (freqArray.isDefined) {
        val freqOld = ItemsSketch.getInstance(Memory.wrap(freqArray.get), new ArrayOfStringsSerDe())
        freqCurrent.merge(freqOld)
      }
      freqCurrent
    }

    def merge(freq1: ItemsSketch[String], freq2: ItemsSketch[String]): ItemsSketch[String] = {
      freq1.merge(freq2)
      freq1
    }

    def finish(freq: ItemsSketch[String]): Array[Byte] = {
      freq.toByteArray(new ArrayOfStringsSerDe())
    }

    def bufferEncoder: Encoder[ItemsSketch[String]] = { Encoders.kryo[ItemsSketch[String]] }

    def outputEncoder: Encoder[Array[Byte]] = { Encoders.BINARY }
  }

  case class mergeFreqsToClass(colName: String, mapSize: Int = defaultMapSize) extends Aggregator[Row, ItemsSketch[String], List[FreqSketch]] with Serializable {
    def zero: ItemsSketch[String] = {
      new ItemsSketch[String](mapSize)
    }
  
    def reduce(freqCurrent: ItemsSketch[String], row: Row): ItemsSketch[String] = {
      val freqArray = getNestedRowValue[Array[Byte]](row, colName)
      if (freqArray.isDefined) {
        val freqOld = ItemsSketch.getInstance(Memory.wrap(freqArray.get), new ArrayOfStringsSerDe())
        freqCurrent.merge(freqOld)
      }
      freqCurrent
    }
  
    def merge(freq1: ItemsSketch[String], freq2: ItemsSketch[String]): ItemsSketch[String] = {
      freq1.merge(freq2)
      freq1
    }
  
    def finish(freq: ItemsSketch[String]): List[FreqSketch] = {
      FreqSketchUdfs.freqSketchToFreqSketch(freq)
    }
  
    def bufferEncoder: Encoder[ItemsSketch[String]] = Encoders.kryo[ItemsSketch[String]]
  
    def outputEncoder: Encoder[List[FreqSketch]] = ExpressionEncoder()
  }
}

object StreamingStatsAggs {
  // Assumes count will always be > 1.
  // Also assumes min value of 0.0 when using nStep option.
  // In general, if either of those things breaks using this agg you should check to make sure something isn't wrong else where first.
  case class stddevStats(colName: String, nSteps: Option[Long] = None) extends Aggregator[Row, IntermediateStddevStats, StddevStats] with Serializable {
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
      val newCount = stddevStats1.count + stddevStats2.count
      val newSum = stddevStats1.sum + stddevStats2.sum
      val delta = stddevStats2.mean - stddevStats1.mean
      val (newMean, newM2) = if (delta != 0 & newCount != 0) {
        val newMean = (stddevStats1.mean * stddevStats1.count + stddevStats2.mean * stddevStats2.count) / newCount
        val newM2 = stddevStats1.m2 + stddevStats2.m2 + pow(delta, 2.0) * stddevStats1.count * stddevStats2.count / newCount

        (newMean, newM2)
      } else {
        (stddevStats1.mean, stddevStats1.m2 + stddevStats2.m2)
      }
      val newMin = (stddevStats1.min, stddevStats2.min) match {
        case (None, None) => None
        case (Some(min1), None) => Some(min1)
        case (None, Some(min2)) => Some(min2)
        case (Some(min1), Some(min2)) => Some(min(min1, min2))
      }

      IntermediateStddevStats(
        count = newCount,
        m2    = newM2,
        max   = max(stddevStats1.max, stddevStats2.max),
        mean  = newMean,
        min   = newMin,
        sum   = newSum
      )
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

      val stddev = sqrt(stats.m2 / (stats.count - 1.0))

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

    def bufferEncoder: Encoder[IntermediateStddevStats] = ExpressionEncoder()

    def outputEncoder: Encoder[StddevStats] = ExpressionEncoder()
  }
}

object UDWFS {
  val defaultMaxSessionLengthSeconds = 60L * 30L

  // Assigns a random UUID to the window for events that are within the sessionWindow of eachother
  // Based on the work done here: https://github.com/rcongiu/spark-udwf-session/blob/master/src/main/scala/com/nuvola_tech/spark/SessionUDWF.scala
  case class SessionUDWF(
    epochSeconds: Expression,
    session: Expression,
    sessionWindow: Expression = Literal(defaultMaxSessionLengthSeconds)
  ) extends AggregateWindowFunction {
    self: Product =>

    override def children: Seq[Expression] = Seq(epochSeconds, session)

    override def dataType: DataType = StringType

    protected val zero = Literal(0L)
    protected val nullString = Literal(null: String)

    protected val currentSession: AttributeReference = AttributeReference("currentSession", StringType, nullable = true)()
    protected val previousES: AttributeReference = AttributeReference("previousES", LongType, nullable = false)()

    override val aggBufferAttributes: Seq[AttributeReference] = currentSession :: previousES :: Nil

    protected val assignSession: Expression = If(
      predicate = LessThanOrEqual(Subtract(epochSeconds, previousES), sessionWindow),
      trueValue = currentSession,
      falseValue = ScalaUDF(createNewSession, StringType, children = Nil, inputsNullSafe = true :: true :: Nil)
    )

    override val initialValues: Seq[Expression] = nullString :: zero :: Nil

    override val updateExpressions: Seq[Expression] =
      If(IsNotNull(session), session, assignSession) ::
        epochSeconds ::
        Nil

    override val evaluateExpression: Expression = currentSession

    override def prettyName: String = "createSession"
  }

  protected val createNewSession = () => org.apache.spark.unsafe.types.UTF8String.fromString(UUID.randomUUID().toString)

  def calculateSession(es: Column, sess: Column): Column = withExpr {
    SessionUDWF(es.expr, sess.expr, Literal(defaultMaxSessionLengthSeconds))
  }

  def calculateSession(es: Column, sess: Column, sessionWindow: Column): Column = withExpr {
    SessionUDWF(es.expr, sess.expr, sessionWindow.expr)
  }

  private def withExpr(expr: Expression): Column = new Column(expr)
}
