package io.opentargets.platform.ddr

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.platform.ddr.algorithms.SimilarityIndex._
import io.opentargets.platform.ddr.algorithms.SimilarityIndex
import org.apache.spark.sql.{DataFrame, SparkSession}


object Relations extends LazyLogging{
  def apply(df: DataFrame, numSynonyms: Int)(implicit ss: SparkSession): Option[DataFrame] = {
    val params = SimilarityIndexParams()
    val algo = new SimilarityIndex(params)

    val diseasesModel =
      algo.fit(df, groupBy = "target_id", aggBy = Seq("disease_id", "disease_label", "score", "count"))

    val targetsModel =
      algo.fit(df, groupBy = "disease_id", aggBy = Seq("target_id", "target_symbol", "score", "count"))

    val dsyns = diseasesModel.map(_.findSynonyms(numSynonyms)(df, "disease_id", "disease_synonyms"))
    val tsyns = targetsModel.map(_.findSynonyms(numSynonyms)(df, "target_id", "target_synonyms"))

    val dfs = List(dsyns, tsyns).filter(_.isDefined).map(_.get)

    // TODO iter all dataframes and generate the structure needed for relations
    None
  }
}
