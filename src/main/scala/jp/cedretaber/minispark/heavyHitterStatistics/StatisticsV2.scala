package jp.cedretaber.minispark.heavyHitterStatistics

import org.apache.spark.sql.DataFrame

object StatisticsV2 extends HeavyHittersStatistics {

  private val THRESHOLD = 4 // worker_instances - 1

  def exec(left: DataFrame, leftColName: String): DataFrame = {
    val counted = left.groupBy(leftColName).count()
    counted.filter(counted("count") >= THRESHOLD).select(leftColName)
  }
}
