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

    val result = users
      .join(relations, users("id") === relations("user_id"))
      .join(langs, relations("lang_id") === langs("id"))
    result.show()

    import org.apache.spark.sql.extensions.FlowJoin._

    val flowResult = users
      .flowJoin(relations, users("id") === relations("user_id"))
      .flowJoin(langs, relations("lang_id") === langs("id"))
    flowResult.show()

    spark.stop()
  }
}
