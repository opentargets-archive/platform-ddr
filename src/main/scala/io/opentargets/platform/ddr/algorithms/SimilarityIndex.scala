package io.opentargets.platform.ddr.algorithms

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.platform.ddr.algorithms.SimilarityIndex.{SimilarityIndexModel, SimilarityIndexParams}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature._
import org.apache.spark.sql.functions.{collect_list, column, mean, udf}
import org.apache.spark.sql.DataFrame


class SimilarityIndex(val df: DataFrame, val params: SimilarityIndexParams) extends LazyLogging {
  def run(groupBy: String, aggBy: Seq[String]): Option[SimilarityIndexModel] = {
    aggregateDF(df, groupBy, aggBy).map(x => {
      val sxx = sortScoredIDs(x._2, x._1.head, x._1(2), x._1.head + "_sorted")
        .persist()

      logger.info(s"aggregated keys count is ${sxx.count} with cNames ${sxx.columns}")

      val w2v = new Word2Vec()
        .setInputCol(x._1.head + "_sorted")
        .setOutputCol("features")
        .setMinCount(params.minWF)

      val w2vModel = w2v.fit(sxx)
      val r = w2vModel.transform(sxx).persist()

      val countWordIDs = w2vModel.getVectors.count

      logger.info(s"words count $countWordIDs")

      SimilarityIndexModel(w2vModel, r.toDF())
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

  private[ddr] def scaleScoredIDs(df: DataFrame, idsColumn: String,
                                  scoresColumn: String, newColumn: String): DataFrame = {
    val transformer = udf((ids: Seq[String], scores: Seq[Double]) =>
      (ids.view zip scores.map(x => math.round(x * 10).toInt).view).flatMap(pair => {
        Seq.fill(pair._2)(pair._1)
      }).force)

    df.withColumn(newColumn, transformer(column(idsColumn), column(scoresColumn)))
  }

  private[ddr] def sortScoredIDs(df: DataFrame, idsColumn: String,
                                 scoresColumn: String, newColumn: String): DataFrame = {
    val transformer = udf((ids: Seq[String], scores: Seq[Double]) =>
      (ids.view zip scores.view).sortBy(-_._2).map(_._1).force)

    df.withColumn(newColumn, transformer(column(idsColumn), column(scoresColumn)))
  }
}

object SimilarityIndex {

  case class SimilarityIndexParams(bucketLen: Double = 2, numHashTables: Int = 10,
                                   binaryMode: Boolean = false, maxDistance: Double = 10,
                                   minWF: Int = 1, minDF: Int = 1)

  case class SimilarityIndexModel(model: Word2VecModel, transformedDF: DataFrame)
}