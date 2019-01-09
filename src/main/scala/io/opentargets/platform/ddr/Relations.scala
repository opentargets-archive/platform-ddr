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
    val algoD = new SimilarityIndex(params)
    val algoT = new SimilarityIndex(params)

    val diseasesModel =
      algoD.fit(df,
        groupBy = "target_id",
        aggBy = Seq("disease_id", "disease_label", "score", "count"))

    val targetsModel =
      algoT.fit(df,
        groupBy = "disease_id",
        aggBy = Seq("target_id", "target_symbol", "score", "count"))

    val targetsDF = df.groupBy("target_id").agg(first("target_symbol").as("target_symbol")).repartition(64).persist()
    val diseasesDF = df.groupBy("disease_id").agg(first("disease_label").as("disease_label")).repartition(64).persist()

    val dsyns = diseasesModel.map(
      _.findSynonyms(numSynonyms)(diseasesDF, "disease_id", "disease_synonyms"))
    val tsyns = targetsModel.map(
      _.findSynonyms(numSynonyms)(targetsDF, "target_id", "target_synonyms"))

    val dfs = List(dsyns, tsyns).filter(_.isDefined).map(_.get)
    logger.debug(s"computed synonym dataframes count ${dfs.length}")

    if (dfs.nonEmpty) {
      Some(dfs.reduce(_ union _))
    } else None
  }
}
