import $ivy.`org.apache.spark::spark-core:2.4.3`
import $ivy.`org.apache.spark::spark-sql:2.4.3`
import $ivy.`org.apache.spark::spark-mllib:2.4.3`
import $ivy.`com.typesafe.scala-logging::scala-logging:3.9.0`
import $ivy.`com.github.fommil.netlib:all:1.1.2`

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util._

case class SimilarityIndexParams(windowSize: Int = 20, minWordFreq: Int = 1, minDocFreq: Int = 2)
case class SimilarityIndexModel(model: Word2VecModel) extends LazyLogging {
  logger.debug(s"created model ${model.uid}")

  def findSynonyms(n: Int)
                  (df: DataFrame, inColName: String, outColName: String)
                  (implicit ss: SparkSession): DataFrame = {
    logger.debug(s"broadcast model ${model.uid}")
    val modelBc = ss.sparkContext.broadcast(model)

    val synUDF = udf((word: String) => {
      Try(modelBc.value.findSynonymsArray(word, n)) match {
        case Success(value) =>
          value
        case Failure(exception) =>
          logger.error(exception.getMessage)
          Array.empty[(String, Double)]
      }
    })

    df.withColumn(outColName, synUDF(col(inColName)))
  }

  def saveTo(path: String): Unit = {
    model.save(path)
  }
}

class SimilarityIndex(val params: SimilarityIndexParams) extends LazyLogging {
  def fit(df: DataFrame, inCol: String): SimilarityIndexModel = {
    /* getting the vector size based on this paper and setting it to 300
      at least there is some number to set somewhere
      https://arxiv.org/pdf/1301.3781.pdf
     */
    val w2v = new Word2Vec()
      .setInputCol(inCol)
      .setOutputCol("features")
      .setMinCount(params.minWordFreq)
      .setWindowSize(params.windowSize)
      .setVectorSize(300)
      .setNumPartitions(10)

    val w2vModel = w2v.fit(df)

    val countWordIDs = w2vModel.getVectors.count
    logger.info(s"model ${w2vModel.uid} words count $countWordIDs")

    SimilarityIndexModel(w2vModel)
  }

  protected def sortScoredIDs(df: DataFrame, idsColumn: String,
                                 scoresColumn: String, newColumn: String): DataFrame = {
    val transformer = udf((ids: Seq[String], scores: Seq[Double]) =>
      (ids.view zip scores.view).sortBy(-_._2).map(_._1).force)

    df.withColumn(newColumn, transformer(col(idsColumn), col(scoresColumn)))
  }
}