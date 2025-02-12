package jp.cedretaber.minispark

import org.apache.spark.sql.SparkSession

object FlowJoinTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("MiniSpark")
      .master("local[*]")
      .getOrCreate()

    val users = spark.read.option("header", true).csv("src/main/resources/users.csv")
    users.show()
    val langs = spark.read.option("header", true).csv("src/main/resources/langs.csv")
    langs.show()
    val relations = spark.read.option("header", true).csv("src/main/resources/user_langs.csv")
    relations.show()

    import org.apache.spark.sql.extensions.FlowJoin2._

    val flowResult = users
      .flowJoin(relations, "id", "user_id")
      .flowJoin(langs, "lang_id", "id")
      .sort("user_id", "lang_id")
    flowResult.show()

    spark.stop()
  }
}
