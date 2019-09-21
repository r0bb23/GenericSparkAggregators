package com.rbb.gsaggs

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{ format_string, col, hash, udf, spark_partition_id }
import org.apache.spark.sql.types.{ ArrayType, DataType, IntegerType, StringType, StructType }
import org.apache.spark.sql.{ Column, DataFrame, Row, SparkSession }
import org.slf4j.LoggerFactory

object SparkDataFrameHelpers {
  val logger = LoggerFactory.getLogger(this.getClass.getName)

  /**
    *  Parse the expression "HLL_CARDINALITY(user_id_hll) AS unique_users" into (HLL_CARDINALITY, user_id_hll, unique_users)
    *  The column expression can in one of three formats: func(colName) AS alias; colName; colName AS alias
    */
  val colExpSimple = raw"""(\w+)""".r
  val colExpAlias = raw"""(\w+)\s+(?i)AS\s+(\w+)""".r
  val colExpFunc = raw"""(\w+)\((\w+)\)\s+(?i)AS\s+(\w+)""".r

  def parseColumnExprs(colExpr: String): (String, String, String) = colExpr match {
    case colExpSimple(colName) => (null, colName, null)
    case colExpAlias(colName, alias) => (null, colName, alias)
    case colExpFunc(func, colName, alias) => (func, colName, alias)
  }

  def extractColNamesFromExprs(colExprs: List[String]): List[String] = {
    colExprs.map(colExpr => {
      colExpr match {
        case colExpSimple(colName) => colName
        case colExpAlias(colName, alias) => alias
        case colExpFunc(func, colName, alias) => alias
      }
    })
  }

  // Merge dataframes
  // Often used in `foldLeft` statements
  def unionDF(df1: Option[DataFrame], df2: DataFrame): Option[DataFrame] = {
    if (df2 == null) {
      df1
    } else if (df1.isEmpty) {
      Some(df2)
    } else {
      Some(df1.get.union(df2))
    }
  }

  def deleteAllTempTables(spark: SparkSession) {
    spark.sqlContext.tables().filter("isTemporary == true").select("tableName").collect.foreach(tblName => spark.catalog.dropTempView(tblName(0).toString))
  }

  def getNestedRowValue[returnType](row: Row, value: String): Option[returnType] = {
    val splitList = value.split("\\.", 2)
    if (row.schema == null) {
      return None
    } else if (splitList.length == 1) {
      return Option(row.getAs[returnType](splitList(0)))
    } else {
      return getNestedRowValue(Option(row.getAs[Row](splitList(0))).getOrElse(Row()), splitList(1))
    }
  }

  /**
    *   Returns an empty dataframe
    *   Useful in folding functions where you just need an empty dataframe to start.
    */
  def getEmptyDF()(implicit spark: SparkSession): DataFrame = {
    val emptySchema = StructType(Seq())
    spark.createDataFrame(spark.sparkContext.emptyRDD[Row], emptySchema)
  }

  /**
    *   Get the value of a row field as a certain type by name.
    */
  def getRowVal[T](r: Row, name: String): T = {
    r.getAs[T](r.fieldIndex(name))
  }

  def toBasicColName(col: String): String = {
    return col.substring(col.lastIndexOf(".") + 1)
  }

  def toBasicColNames(cols: List[String]): List[String] = {
    return cols.map(col => toBasicColName(col))
  }

  def stringListsToCols(stringLists: List[String]*): List[Column] = {
    val strings = stringLists.flatten.toList
    return strings.map(element => col(element))
  }

  /**
    * Get the number of partitions for a dataframe via the harmonic mean of ideal size in bytes,
    * idea size wrt available cores, and current number of partitions.
    */
  def optimalNumberOfPartitions(dataset: DataFrame): Int = {
    import org.apache.spark.util.SizeEstimator

    // 4 partitions per core
    val parallelism = dataset.sqlContext.sparkContext.defaultParallelism
    val partitionSizePar = 4 * parallelism

    // 1 partition per 64 mb of data
    val bytesPerPartition = FileSystemGlobals.BLOCK_SIZE
    val partitionSizeBytes = math.ceil(SizeEstimator.estimate(dataset) / bytesPerPartition)

    // The current number of partitions
    val parttionsSize = dataset.rdd.partitions.size

    val hmean = MathHelpers.harmonicMean(Seq(partitionSizePar, partitionSizeBytes, parttionsSize))

    if (hmean < partitionSizeBytes) {
      return math.ceil(partitionSizeBytes).toInt
    } else if (hmean < partitionSizePar) {
      return partitionSizePar
    } else {
      return math.ceil(hmean).toInt
    }
  }

  // Returns a dataframe with partition_id, ct
  def sizePerPartition(df: DataFrame): DataFrame = {
    df.groupBy(spark_partition_id).count
  }

  def coalesce(df: DataFrame, rowCount: Long, maxRowsPerPartition: Long = FileSystemGlobals.MAX_RECORDS_PER_FILE): DataFrame = {
    val partitionSize = rowCount / maxRowsPerPartition + 1
    if (df.rdd.partitions.size <= partitionSize) {
      df
    } else {
      df.coalesce(partitionSize.toInt)
    }
  }

  /**
    * Partition a dataset by a column.
    *  If the number of items in a single partition bucket is > maxRecordsPerPartitionGroup, split that group.
    *  into multiple smaller groups.  This helps keep writes efficient when indivdual files can get large in some hot
    *  partitions by breaking writes into multiple smaller files (e.g. csv of key activity per tenant).
    */
  def repartition(
      df:          DataFrame,
      partColName: String, subPartColNames: Seq[String],
      maxRecordsPerPartitionGroup: Int, npartitions: Option[Int] = None
  ): DataFrame = {

    val itemsPerGrouping = df
      .groupBy(partColName)
      .count
      .filter(col("count") > maxRecordsPerPartitionGroup)
      .collect

    // How many partitions should each group be broken into?
    // Broadcast this value so it can be efficiently used as a lookup table in udf
    val subPartitionsPerGrouping = df.sparkSession.sparkContext.broadcast[Map[Any, Int]](itemsPerGrouping.map {
      case row => {
        row.getAs[Any](0) -> Math.ceil(row.getLong(1).toDouble / maxRecordsPerPartitionGroup).toInt
      }
    }.toMap)

    logger.info(s"Found ${itemsPerGrouping.length} partitions of size > ${maxRecordsPerPartitionGroup}, which need to be split.")

      // Custom partitioning is not directly available for dataframes, so we need to use a udf
      // https://stackoverflow.com/questions/42071362/spark-dataset-custom-partitioner
      def partitionByCol(partitionColVal: Any, subPartHash: Int): Int = {
        // Calculate # parts for this
        val nparts = subPartitionsPerGrouping.value.getOrElse(partitionColVal, 1)

        // Return an integer value
        return Math.abs(subPartHash % nparts)
      }

    // https://stackoverflow.com/questions/35546576/how-can-i-pass-extra-parameters-to-udfs-in-sparksql
    val partitionByColUdf = udf((p: String, sp: Int) => partitionByCol(p, sp))

    df.repartition(
      npartitions.getOrElse(df.sparkSession.sparkContext.defaultParallelism),
      col(partColName),
      partitionByColUdf(
        col(partColName),
        hash(subPartColNames.map(col): _*)
      )
    )
  }

  // Get basic DataType of a field
  // Example usage: getBasicDataType(df.schema, "datime.date")
  // Example usage: getBasicDataType(df.schema, "files.element.extension")
  def getBasicDataType(dtype: DataType, name: String): DataType = {
    val names = name.split('.')
    val root = names(0)
    val newName = names.drop(1).mkString(".")
    dtype match {
      case dtype: StructType => {
        val newType = dtype.find(_.name == root).get.dataType
        getBasicDataType(newType, newName)
      }
      case array: ArrayType => {
        val newType = array.elementType
        getBasicDataType(newType, newName)
      }
      case _ => dtype
    }
  }

  // Get a String column by concatenating multiple columns
  def getFormatString(schema: DataType, colNames: List[String]): Column = {
    val format = colNames.map(name => getBasicDataType(schema, name) match {
      case StringType => "%s"
      case IntegerType => "%d"
      case _ => new Exception("Exception in getDataType")
    }).mkString(":")
    val cols = colNames.map(name => (col(name)))
    format_string(format, cols: _*)
  }
}
