package org.apache.spark.sql.extensions

import org.apache.spark.sql.extensions.functions.shuffle_hash
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.{Column, DataFrame, Dataset}

object FlowJoin {
  def exec(left: DataFrame, right: DataFrame, joinExprs: Column): DataFrame = {
    // TODO: heavy hitterでちゃんとsplitする
    val left_sc = left
    val left_br = left
    val right_sc = right
    val right_br = right

    val sc = left_sc.join(shuffle_hash(right_sc), joinExprs)
    val br = left_br.join(broadcast(right_br), joinExprs)

    // splitをサボった分を消す
    // TODO: heavy hitterでちゃんとsplitしたらdistinctを消す
    sc.union(br).distinct()
  }

  implicit class extension(val left: DataFrame) extends AnyVal {
    def flowJoin(right: DataFrame, joinExprs: Column): DataFrame = exec(left, right, joinExprs)
  }
}
