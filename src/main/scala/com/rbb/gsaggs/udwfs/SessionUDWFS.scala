package com.rbb.gsaggs.udwfs

import java.util.UUID
import org.apache.spark.sql.catalyst.expressions.{
  AggregateWindowFunction,
  AttributeReference,
  Expression,
  If,
  IsNotNull,
  LessThanOrEqual,
  Literal,
  ScalaUDF,
  Subtract
}
import org.apache.spark.sql.types._
import org.apache.spark.sql.Column
import org.apache.spark.unsafe.types.UTF8String
import scala.collection.JavaConversions._

object SessionUDWFS {
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

  def calculateSession(
      es: Column,
      sess: Column
  ): Column = withExpr {
    SessionUDWF(es.expr, sess.expr, Literal(defaultMaxSessionLengthSeconds))
  }

  def calculateSession(
      es: Column,
      sess: Column,
      sessionWindow: Column
  ): Column = withExpr {
    SessionUDWF(es.expr, sess.expr, sessionWindow.expr)
  }

  private def withExpr(expr: Expression): Column = new Column(expr)
}
