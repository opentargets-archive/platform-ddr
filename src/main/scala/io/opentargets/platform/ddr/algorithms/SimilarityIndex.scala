package io.opentargets.platform.ddr.algorithms

import breeze.linalg.Vector
import com.typesafe.scalalogging.LazyLogging
import io.opentargets.platform.ddr.algorithms.SimilarityIndex.{SimilarityIndexModel, SimilarityIndexParams}
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.functions.{collect_list, column, mean, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}


class SimilarityIndex(val params: SimilarityIndexParams) extends LazyLogging {
  def fit(df: DataFrame, groupBy: String, aggBy: Seq[String]): Option[SimilarityIndexModel] = {
    aggregateDF(df, groupBy, aggBy).map(x => {
      val persistedDF = x._2.persist()

      logger.debug(s"aggregated keys count is ${persistedDF.count} with cNames ${persistedDF.columns}")

      // compute frequency of words
      val cvModel: CountVectorizerModel = new CountVectorizer()
        .setInputCol(x._1.head)
        .setOutputCol("cv")
        .setMinDF(params.minDocFreq)
        .fit(persistedDF)

      // transform the DF using the already computed frequency model
      val sxxCV = cvModel
        .transform(persistedDF)
        .persist()

      // compute IDF model from the word frequency transformed DF
      val idfModel = new IDF()
        .setMinDocFreq(params.minDocFreq)
        .setInputCol("cv")
        .setOutputCol("idf")
        .fit(sxxCV)

      // transform the dataframe and obtain the IDF scores per word in each row
      val sxxCVIDF = idfModel.transform(sxxCV)

      val sortedDF = sortScoredIDs(scaleScoresByIDF(sxxCVIDF, x._1(2), "idf", "scaled_scores"),
        x._1.head, "scaled_scores", x._1.head + "_sorted")
        .persist()

      sortedDF.show(10, truncate = false)

      val w2v = new Word2Vec()
        .setInputCol(x._1.head + "_sorted")
        .setOutputCol("features")
        .setMinCount(params.minWordFreq)
        .setWindowSize(params.windowSize)

      val w2vModel = w2v.fit(sortedDF)

      val countWordIDs = w2vModel.getVectors.count
      logger.info(s"model ${w2vModel.uid} words count $countWordIDs")

      SimilarityIndexModel(w2vModel)
    })
  }

  private[ddr] def aggregateDF(df: DataFrame, groupBy: String, aggBy: Seq[String]): Option[(Seq[String], DataFrame)] = {
    if (aggBy.nonEmpty) {
      val colNames = aggBy.map(_ + "_list")
      val aggL = (aggBy zip colNames).map(elem => collect_list(elem._1).as(elem._2))

      val filteredDF = df.groupBy(column(groupBy))
        .agg(aggL.head, aggL.tail: _*)
      Some((colNames, filteredDF))
    } else None
  }

  private[ddr] def scaleScoresByIDF(df: DataFrame, scores: String, idfScores: String, newColumn: String): DataFrame = {
    val transformer = udf((scores: Seq[Double], idfs: SparseVector) =>
      (scores.view zip idfs.values.view).map(p => p._1 * p._2).force)

    df.withColumn(newColumn, transformer(column(scores), column(idfScores)))
  }

  private[ddr] def sortScoredIDs(df: DataFrame, idsColumn: String,
                                 scoresColumn: String, newColumn: String): DataFrame = {
    val transformer = udf((ids: Seq[String], scores: Seq[Double]) =>
      (ids.view zip scores.view).sortBy(-_._2).map(_._1).force)

    df.withColumn(newColumn, transformer(column(idsColumn), column(scoresColumn)))
  }
}

object SimilarityIndex {

  case class SimilarityIndexParams(windowSize: Int = 5, minWordFreq: Int = 1, minDocFreq: Int = 1)

  case class SimilarityIndexModel(model: Word2VecModel) extends LazyLogging {
    logger.debug(s"created model ${model.uid}")

    def findSynonyms(n: Int)
                    (df: DataFrame, inColName: String, outColName: String)
                    (implicit ss: SparkSession): DataFrame = {
      logger.debug(s"broadcast model ${model.uid}")
      val modelBc = ss.sparkContext.broadcast(model)

      val synUDF = udf((word: String) => modelBc.value.findSynonymsArray(word, n))

      df.withColumn(outColName, synUDF(column(inColName)))
    }
  }

}