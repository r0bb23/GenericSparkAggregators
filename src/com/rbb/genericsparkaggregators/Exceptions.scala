package com.rbb.genericsparkaggregators

object Exceptions {
  case class NotValidType(
    message: String
  ) extends Exception(message)

  case class DatasetWriteError(
    message: String
  ) extends Exception(message)

  case class NotValidValue(
    message: String
  ) extends Exception(message)

  case class NotRegisteredFunction(
    func: String
  ) extends Exception(s"No registered function named: $func.")

  case class NotRegisteredTransformer(
    transformer: String
  ) extends Exception(s"No registered transformer named: $transformer.")

  case class MissingArguments(
    who:  String,
    args: String
  ) extends Exception(s"$who missing arguments: $args.")

  case class InvalidColumnName(
    cols: String
  ) extends Exception(s"Invalid column names :$cols.")

  case class MutuallyExclusiveParameters(
    parameters: List[String]
  ) extends Exception(s"More than one of the following parameters was set: ${parameters.mkString(", ")}. Only one can be set at a time.")
}
