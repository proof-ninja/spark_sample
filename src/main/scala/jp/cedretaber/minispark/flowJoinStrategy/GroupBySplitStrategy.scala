package jp.cedretaber.minispark.flowJoinStrategy

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.extensions.functions.shuffle_hash
import org.apache.spark.sql.functions.broadcast

import scala.jdk.CollectionConverters._

object GroupBySplitStrategy {
  private def split[T](table: DataFrame, colName: String, heavyHitters: Broadcast[Set[T]]): (DataFrame, DataFrame) = {
    val idx = table.schema.fieldIndex(colName)
    val groups = table.rdd.groupBy(row => heavyHitters.value(row.getAs[T](idx))).collectAsMap()
    val sparkSession = table.sparkSession
    (
      sparkSession.createDataFrame(groups(true).toList.asJava, table.schema),
      sparkSession.createDataFrame(groups(false).toList.asJava, table.schema)
    )
  }

  def exec[T](
    left: DataFrame,
    right: DataFrame,
    leftColName: String,
    rightColName: String,
    heavyHitters: Broadcast[Set[T]]
  ): DataFrame = {
    val (leftBr, leftSc) = split(left, leftColName, heavyHitters)
    val (rightBr, rightSc) = split(right, rightColName, heavyHitters)

    val sc = leftSc.join(shuffle_hash(rightSc), leftSc(leftColName) === rightSc(rightColName))
    val br = leftBr.join(broadcast(rightBr), leftBr(leftColName) === rightBr(rightColName))

    sc.union(br)
  }

  implicit class extension(val left: DataFrame) {
    def flowJoin[T](
      right: DataFrame,
      leftColName: String,
      rightColName: String,
      heavyHitters: Broadcast[Set[T]]
    ): DataFrame = exec(left, right, leftColName, rightColName, heavyHitters)
  }
}
