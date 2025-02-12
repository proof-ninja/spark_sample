package org.apache.spark.sql.extensions

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.extensions.functions.shuffle_hash
import org.apache.spark.sql.functions.broadcast

import scala.jdk.CollectionConverters.SeqHasAsJava

object FlowJoin2 {
  def exec(sparkSession: SparkSession, left: DataFrame, right: DataFrame, leftColName: String, rightColName: String): DataFrame = {
    val sparkSession = left.sparkSession

    val counts = left
      .select(leftColName)
      .union(right.select(rightColName))
      .collect()
      .map(_.get(0))
      .groupBy(identity)
      .view
      .mapValues(_.length)
      .toMap
    val heavyHitters = counts.filter(_._2 > 2).keys.toSet // 仮なので雑に複数回出てきたらheavy hitterということにしちゃう
    val bh = sparkSession.sparkContext.broadcast(heavyHitters)
    println(heavyHitters)
    println(bh.value)

    val leftIdx = left.schema.fieldIndex(leftColName)
    val rightIdx = right.schema.fieldIndex(rightColName)

    val lefts = left.rdd.groupBy(r => heavyHitters(r.get(leftIdx))).collectAsMap()
    val rights = right.rdd.groupBy(r => heavyHitters(r.get(rightIdx))).collectAsMap()
    println(lefts)
    println(rights)

    val left_sc = sparkSession.createDataFrame(lefts(false).toSeq.asJava, left.schema)
    val left_br = sparkSession.createDataFrame(lefts(true).toSeq.asJava, left.schema)
    val right_sc = sparkSession.createDataFrame(rights(false).toSeq.asJava, right.schema)
    val right_br = sparkSession.createDataFrame(rights(true).toSeq.asJava, right.schema)

    val sc = left_sc.join(shuffle_hash(right_sc), left_sc(leftColName) === right_sc(rightColName))
    val br = left_br.join(broadcast(right_br), left_br(leftColName) === right_br(rightColName))



    sc.union(br)
  }

  implicit class extension(val left: DataFrame) extends AnyVal {
    def flowJoin(right: DataFrame, leftColName: String, rightColName: String): DataFrame =
      exec(left.sparkSession, left, right, leftColName, rightColName)
  }
}
