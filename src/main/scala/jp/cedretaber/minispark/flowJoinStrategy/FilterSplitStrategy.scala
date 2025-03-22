package jp.cedretaber.minispark.flowJoinStrategy

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.extensions.functions.shuffle_hash
import org.apache.spark.sql.functions.broadcast

object FilterSplitStrategy {
  private def split[T](table: DataFrame, colName: String, heavyHitters: Broadcast[Set[T]]): (DataFrame, DataFrame) = {
    val idx = table.schema.fieldIndex(colName)
    (
      table.filter(row => heavyHitters.value(row.getAs[T](idx))),
      table.filter(row => !heavyHitters.value(row.getAs[T](idx)))
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
