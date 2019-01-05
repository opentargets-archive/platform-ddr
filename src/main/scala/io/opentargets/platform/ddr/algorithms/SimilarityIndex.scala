package io.opentargets.platform.ddr.algorithms

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.platform.ddr.algorithms.SimilarityIndex.SimilarityIndexParams
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature._
import org.apache.spark.sql.functions.{collect_list, column, mean, udf}
import org.apache.spark.sql.DataFrame


class SimilarityIndex(val df: DataFrame, val params: SimilarityIndexParams) extends LazyLogging {
  def run(groupBy: String, aggBy: Seq[String]): Option[DataFrame] = {
    aggregateDF(df, groupBy, aggBy).map(x => {
      val sx = scaleScoredIDs(x._2, x._1.head, x._1(2), x._1.head + "_scaled")
        .persist()

      sx.show(100, truncate = false)

      sx.where(column(groupBy) === "ENSG00000167207").show(100, truncate = false)

      logger.info(s"aggregated keys count is ${sx.count} with cNames ${sx.columns}")

      val tf: HashingTF = new HashingTF()
        .setInputCol(x._1.head + "_scaled")
        .setOutputCol("tf")
        .setBinary(params.binaryMode)

      val idf = new IDF()
        .setInputCol("tf")
        .setOutputCol("tf_idf")

      val brp = new BucketedRandomProjectionLSH()
        .setBucketLength(params.bucketLen)
        .setNumHashTables(params.numHashTables)
        .setInputCol("tf_idf")
        .setOutputCol("hashes")

      val pipeline = new Pipeline()
        .setStages(Array(tf, idf))

      val tx = pipeline.fit(sx)
        .transform(sx)

      val brp_model = brp.fit(tx)

      val ttdf = brp_model.transform(tx)

      val r = brp_model.approxSimilarityJoin(ttdf, ttdf, params.maxDistance)
        .where(column(s"datasetA.$groupBy") =!= column(s"datasetB.$groupBy"))
        .persist()

      r.show(100, truncate = false)
      r.where(column(s"datasetA.$groupBy") === "ENSG00000167207").show(100, truncate = false)

      logger.info(s"approx similarity join count ${r.count}")

      r.toDF()
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
}

object SimilarityIndex {

  case class SimilarityIndexParams(bucketLen: Double = 2, numHashTables: Int = 10,
                                   binaryMode: Boolean = false, maxDistance: Double = 10)

}