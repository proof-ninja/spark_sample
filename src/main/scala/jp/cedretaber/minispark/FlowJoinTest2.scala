package jp.cedretaber.minispark

import jp.cedretaber.minispark.heavyHitterStatistics.StatisticsV2
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import scala.jdk.CollectionConverters._

// heavy hitters の計算を与えるサンプル
object FlowJoinTest2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("MiniSpark")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val users = spark.read.option("header", true).csv("src/main/resources/users.csv")
    users.show()
    val langs = spark.read.option("header", true).csv("src/main/resources/langs.csv")
    langs.show()
    val relations = spark.read.option("header", true).csv("src/main/resources/user_langs.csv")
    relations.show()

    {
      import jp.cedretaber.minispark.flowJoinStrategy.JoinSplitStrategy._

      val result = users
        .flowJoin[String](relations, "id", "user_id", StatisticsV2)
        .flowJoin[String](langs, "lang_id", "id", StatisticsV2)
      result.show()
    }

    spark.stop()
  }
}
