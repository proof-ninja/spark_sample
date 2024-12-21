

import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("MiniSpark")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

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

    spark.stop()
  }
}
