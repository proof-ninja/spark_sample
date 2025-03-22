package jp.cedretaber.minispark


import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import scala.jdk.CollectionConverters._

// 各種splitのサンプル
object FlowJoinTest {
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
      import jp.cedretaber.minispark.flowJoinStrategy.IteratorSplitStrategy._

      val result = users
        .flowJoin[String](relations, "id", "user_id", Array("1", "3", "4"))
        .flowJoin[String](langs, "lang_id", "id", Array("1", "5"))
      result.show()
    }

    {
      import jp.cedretaber.minispark.flowJoinStrategy.GroupBySplitStrategy._

      val result = users
        .flowJoin[String](relations, "id", "user_id", spark.sparkContext.broadcast(Set("1", "3", "4")))
        .flowJoin[String](langs, "lang_id", "id", spark.sparkContext.broadcast(Set("1", "5")))
      result.show()
    }

    {
      import jp.cedretaber.minispark.flowJoinStrategy.FilterSplitStrategy._

      val result = users
        .flowJoin[String](relations, "id", "user_id", spark.sparkContext.broadcast(Set("1", "3", "4")))
        .flowJoin[String](langs, "lang_id", "id", spark.sparkContext.broadcast(Set("1", "5")))
      result.show()
    }

    {
      import jp.cedretaber.minispark.flowJoinStrategy.JoinSplitStrategy._

      val schema = StructType(Seq(StructField("id", StringType)))

      val result = users
        .flowJoin[String](relations, "id", "user_id", spark.createDataFrame(Seq("1", "3", "4").map(Row(_: _*)).asJava, schema))
        .flowJoin[String](langs, "lang_id", "id", spark.createDataFrame(Seq("1", "5").map(Row(_: _*)).asJava, schema))
      result.show()
    }

    spark.stop()
  }
}
