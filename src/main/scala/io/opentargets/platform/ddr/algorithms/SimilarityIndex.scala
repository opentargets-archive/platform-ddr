package io.opentargets.platform.ddr.algorithms

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.platform.ddr.algorithms.SimilarityIndex.{SimilarityIndexModel, SimilarityIndexParams}
import org.apache.spark.ml.feature._
import org.apache.spark.sql.functions.{collect_list, column, mean, udf, struct}
import org.apache.spark.sql.{DataFrame, SparkSession}


class SimilarityIndex(val params: SimilarityIndexParams) extends LazyLogging {
  def fit(df: DataFrame, groupBy: String, aggBy: Seq[String]): Option[SimilarityIndexModel] = {
    aggregateDF(df, groupBy, aggBy).map(x => {
      val sxx = sortScoredIDs(x._2, x._1.head, x._1(2), x._1.head + "_sorted")
        .persist()

      logger.debug(s"aggregated keys count is ${sxx.count} with cNames ${sxx.columns}")

      val w2v = new Word2Vec()
        .setInputCol(x._1.head + "_sorted")
        .setOutputCol("features")
        .setMinCount(params.minWordFreq)
        .setWindowSize(params.windowSize)

      val w2vModel = w2v.fit(sxx)
      // val r = w2vModel.transform(sxx).persist()

      val countWordIDs = w2vModel.getVectors.count

      logger.info(s"model ${w2vModel.uid} words count $countWordIDs")

      SimilarityIndexModel(w2vModel)
    })
  }

  private[ddr] def aggregateDF(df: DataFrame, groupBy: String, aggBy: Seq[String]): Option[(Seq[String], DataFrame)] = {
    // TODO it is not well coded the way I use seqs assuming specific lengths
    if (aggBy.nonEmpty) {
      val colNames = aggBy.map(_ + "_list") ++ Seq("mean_score", "mean_count")
      val aggL = (aggBy zip colNames).take(aggBy.size).map(elem => collect_list(elem._1).as(elem._2)) ++
        Seq(mean(aggBy(2)).as("mean_score"), mean(aggBy(3)).as("mean_count"))

      val filteredDF = df.groupBy(column(groupBy))
        .agg(aggL.head, aggL.tail: _*)
      Some((colNames, filteredDF))
    } else None
  }

  private[ddr] def sortScoredIDs(df: DataFrame, idsColumn: String,
                                 scoresColumn: String, newColumn: String): DataFrame = {
    val transformer = udf((ids: Seq[String], scores: Seq[Double]) =>
      (ids.view zip scores.view).sortBy(-_._2).map(_._1).force)

    df.withColumn(newColumn, transformer(column(idsColumn), column(scoresColumn)))
  }
}

object SimilarityIndex {

  case class SimilarityIndexParams(windowSize: Int = 5, minWordFreq: Int = 1)

  case class SimilarityIndexModel(model: Word2VecModel) extends LazyLogging {
    logger.debug(s"created model ${model.uid}")

    def findSynonyms(n: Int)
                    (df: DataFrame, inColName: String, outColName: String)
                    (implicit ss: SparkSession): DataFrame = {
      logger.debug(s"broadcast model ${model.uid}")
      val modelBc = ss.sparkContext.broadcast(model)

      val synUDF = udf((word: String) => {
        val m = modelBc.value
        m.findSynonyms(word, n).rdd.map(r => (r.getAs[String](0), r.getAs[Double](1))).collect()
      })

      df.withColumn(outColName, synUDF(column(inColName)))
    }
  }

}