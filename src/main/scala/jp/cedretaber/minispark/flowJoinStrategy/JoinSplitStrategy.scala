package jp.cedretaber.minispark.flowJoinStrategy

import jp.cedretaber.minispark.heavyHitterStatistics.HeavyHittersStatistics
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.extensions.functions.shuffle_hash
import org.apache.spark.sql.functions.broadcast

object JoinSplitStrategy {
  private def split[T](table: DataFrame, colName: String, heavyHitters: DataFrame): (DataFrame, DataFrame) = {
    val heavyHittersColName = heavyHitters.columns(0)
    (
      table.join(heavyHitters, table(colName) === heavyHitters(heavyHittersColName), "left_semi"),
      table.join(heavyHitters, table(colName) === heavyHitters(heavyHittersColName), "left_anti")
    )
  }

  def exec[T](
    left: DataFrame,
    right: DataFrame,
    leftColName: String,
    rightColName: String,
    heavyHitters: DataFrame
  ): DataFrame = {
    val (leftBr, leftSc) = split(left, leftColName, heavyHitters)
    val (rightBr, rightSc) = split(right, rightColName, heavyHitters)

    val sc = leftSc.join(shuffle_hash(rightSc), leftSc(leftColName) === rightSc(rightColName))
    val br = leftBr.join(broadcast(rightBr), leftBr(leftColName) === rightBr(rightColName))

    sc.union(br)
  }

  implicit class extension(val left: DataFrame) {
    /**
     * heavy hitters 外挿版
     */
    def flowJoin[T](
      right: DataFrame,
      leftColName: String,
      rightColName: String,
      heavyHitters: DataFrame
    ): DataFrame = exec(left, right, leftColName, rightColName, heavyHitters)

    /**
     * heavy hitters 計算版
     */
    def flowJoin[T](
      right: DataFrame,
      leftColName: String,
      rightColName: String,
      heavyHittersStatistics: HeavyHittersStatistics
    ): DataFrame =
      exec(left, right, leftColName, rightColName, heavyHittersStatistics.exec(left, leftColName))
  }
}
