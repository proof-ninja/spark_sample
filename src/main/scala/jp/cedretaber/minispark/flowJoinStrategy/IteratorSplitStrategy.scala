package jp.cedretaber.minispark.flowJoinStrategy

import org.apache.spark.sql.extensions.functions.shuffle_hash
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.{DataFrame, Row}

import scala.jdk.CollectionConverters._
import scala.math.Ordering.Implicits._

object IteratorSplitStrategy {
  private def split[T: Ordering](table: DataFrame, colName: String, heavyHitters: Array[T]): (DataFrame, DataFrame) = {
    val idx = table.schema.fieldIndex(colName)
    val brBuilder = Seq.newBuilder[Row]
    val shBuilder = Seq.newBuilder[Row]

    var heavyHitterIndex = 0
    for {
      row <- table.rdd.collect().sortBy(_.getAs[T](idx))
    } {
      while (heavyHitterIndex < heavyHitters.length && heavyHitters(heavyHitterIndex) < row.getAs[T](idx)) {
        heavyHitterIndex += 1
      }

      if (heavyHitterIndex < heavyHitters.length && heavyHitters(heavyHitterIndex) == row.getAs[T](idx)) {
        brBuilder += row
      } else {
        shBuilder += row
      }
    }

    val sparkSession = table.sparkSession
    (
      sparkSession.createDataFrame(brBuilder.result().asJava, table.schema),
      sparkSession.createDataFrame(shBuilder.result().asJava, table.schema)
    )
  }


  def exec[T: Ordering](
    left: DataFrame,
    right: DataFrame,
    leftColName: String,
    rightColName: String,
    heavyHitters: Array[T]
  ): DataFrame = {
    val (leftBr, leftSc) = split(left, leftColName, heavyHitters)
    val (rightBr, rightSc) = split(right, rightColName, heavyHitters)

    val sc = leftSc.join(shuffle_hash(rightSc), leftSc(leftColName) === rightSc(rightColName))
    val br = leftBr.join(broadcast(rightBr), leftBr(leftColName) === rightBr(rightColName))

    sc.union(br)
  }

  implicit class extension(val left: DataFrame) {
    def flowJoin[T: Ordering](
      right: DataFrame,
      leftColName: String,
      rightColName: String,
      heavyHitters: Array[T]
    ): DataFrame = exec(left, right, leftColName, rightColName, heavyHitters)
  }
}
