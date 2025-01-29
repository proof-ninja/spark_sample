package org.apache.spark.sql.extensions

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.catalyst.plans.logical._

/**
 * join hintを付加する関数群
 *
 * @note DataSet#apply が package private なので org.apache.spark.sql 以下に置いている
 */
object functions {
  def no_broadcast_hash[T](df: Dataset[T]): Dataset[T] = {
    Dataset[T](df.sparkSession,
      ResolvedHint(df.logicalPlan, HintInfo(strategy = Some(NO_BROADCAST_HASH))))(df.exprEnc)
  }

  def prefer_shuffle_hash[T](df: Dataset[T]): Dataset[T] = {
    Dataset[T](df.sparkSession,
      ResolvedHint(df.logicalPlan, HintInfo(strategy = Some(PREFER_SHUFFLE_HASH))))(df.exprEnc)
  }

  def shuffle_hash[T](df: Dataset[T]): Dataset[T] = {
    Dataset[T](df.sparkSession,
      ResolvedHint(df.logicalPlan, HintInfo(strategy = Some(SHUFFLE_HASH))))(df.exprEnc)
  }
}
