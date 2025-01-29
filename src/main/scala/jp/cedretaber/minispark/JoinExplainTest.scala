package jp.cedretaber.minispark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.extensions.functions.{no_broadcast_hash, prefer_shuffle_hash, shuffle_hash}
import org.apache.spark.sql.functions.broadcast

object JoinExplainTest {
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

    // BroadcastHashJoin になった
    val result = users
      .join(relations, users("id") === relations("user_id"))
      .join(langs, relations("lang_id") === langs("id"))
    result.show()
    result.explain()

    // BroadcastHashJoin になった
    val broadcastResult = users
      .join(broadcast(relations), users("id") === relations("user_id"))
      .join(broadcast(langs), relations("lang_id") === langs("id"))
    broadcastResult.show()
    broadcastResult.explain()

    // BroadcastHashJoin になった
    val noBroadcastHashResult = users
      .join(no_broadcast_hash(relations), users("id") === relations("user_id"))
      .join(no_broadcast_hash(langs), relations("lang_id") === langs("id"))
    noBroadcastHashResult.show()
    noBroadcastHashResult.explain()

    // BroadcastHashJoin になった
    val preferShuffleHashResult = users
      .join(prefer_shuffle_hash(relations), users("id") === relations("user_id"))
      .join(prefer_shuffle_hash(langs), relations("lang_id") === langs("id"))
    preferShuffleHashResult.show()
    preferShuffleHashResult.explain()

    // これだけは ShuffledHashJoin になった
    val shuffleHashResult = users
      .join(shuffle_hash(relations), users("id") === relations("user_id"))
      .join(shuffle_hash(langs), relations("lang_id") === langs("id"))
    shuffleHashResult.show()
    shuffleHashResult.explain()

    spark.stop()
  }
}
