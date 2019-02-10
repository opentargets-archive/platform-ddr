package io.opentargets.platform.ddr

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.platform.ddr.algorithms.SimilarityIndex._
import io.opentargets.platform.ddr.algorithms.SimilarityIndex
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object Relations extends LazyLogging {
  def apply(df: DataFrame, numSynonyms: Int)(
    implicit ss: SparkSession): Option[DataFrame] = {
    val params = SimilarityIndexParams()
    val algo = new SimilarityIndex(params)

    val objectModel =
      algo.fit(df,
        groupBy = "object",
        aggBy = Seq("subject", "score"))

    val subjects = df.select(col("subject"))
      .distinct()
      .repartitionByRange(col("subject"))
      .persist()

    val syns = objectModel.map(
      _.findSynonyms(numSynonyms)(subjects, "subject", "subject_synonyms"))

    val dfs = List(syns).withFilter(_.isDefined).map(_.get)
    logger.debug(s"computed synonym dataframes count ${dfs.length}")

    if (dfs.nonEmpty) {
      Some(dfs.reduce(_ union _))
    } else None
  }
}
