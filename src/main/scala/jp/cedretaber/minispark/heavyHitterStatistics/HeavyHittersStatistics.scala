package jp.cedretaber.minispark.heavyHitterStatistics

import org.apache.spark.sql.DataFrame

trait HeavyHittersStatistics {
  def exec(left: DataFrame, leftColName: String): DataFrame
}
