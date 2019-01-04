package io.opentargets.platform.ddr.algorithms

import com.typesafe.scalalogging.LazyLogging
import io.opentargets.platform.ddr.algorithms.SimilarityIndex.SimilarityIndexParams
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature._
import org.apache.spark.sql.functions.{collect_list, column, mean}
import org.apache.spark.sql.DataFrame


class SimilarityIndex(val df: DataFrame, val params: SimilarityIndexParams) extends LazyLogging {
  def run(groupBy: String, aggBy: Seq[String]): Option[(Seq[String], DataFrame)] = {
    aggregateDF(df, groupBy, aggBy).map(x => {
      logger.info(s"aggregated keys count is ${x._2.count()}")

      val cv: CountVectorizer = new CountVectorizer()
        .setInputCol(aggBy.head + "_list")
        .setOutputCol("tf")
        .setMinDF(params.minDF)
        .setBinary(params.binaryMode)

      val idf = new IDF()
        .setInputCol("tf")
        .setOutputCol("tf_idf")

      val percentileDisct = new QuantileDiscretizer()
        .setInputCol("mean_count")
        .setOutputCol("q_count")
        .setNumBuckets(params.numPercentiles)

      // Seq("disease_id", "disease_label", "score", "count", "mean_score", "mean_count")
      val assembler = new VectorAssembler()
        .setInputCols(Array("tf_idf", /*"mean_score",*/ "q_count"))
        .setOutputCol("features")

      val brp = new BucketedRandomProjectionLSH()
        .setBucketLength(params.bucketLen)
        // .setNumHashTables(3)
        .setInputCol("features")
        .setOutputCol("hashes")

      val pipeline = new Pipeline()
        .setStages(Array(cv, idf, percentileDisct, assembler))

      val tx = pipeline.fit(x._2)
        .transform(x._2)

      val brp_model = brp.fit(tx)

      val ttdf = brp_model.transform(tx)

      val r = brp_model.approxSimilarityJoin(ttdf, ttdf, params.threshold)
        .where(column(s"datasetA.${groupBy}") =!= column(s"datasetB.${groupBy}"))
        .persist()

      logger.info(s"approx similarity join count ${r.count()}")

      (x._1, r.toDF())
    })
  }

  private[ddr] def aggregateDF(df: DataFrame, groupBy: String, aggBy: Seq[String]): Option[(Seq[String], DataFrame)] = {
    // TODO it is not well coded the way I use seqs assuming specific lengths
    if (aggBy.nonEmpty) {
      val colNames = aggBy.map(_ + "_list") ++ Seq("mean_score", "mean_count")
      val aggL = (aggBy zip colNames).take(aggBy.size).map(elem => collect_list(elem._1).as(elem._2)) ++
        Seq(mean(aggBy(2)).as("mean_score"), mean(aggBy(3)).as("mean_count"))

      val filteredDF = df.groupBy(column(groupBy))
        .agg(aggL.head, aggL.tail: _*).persist()
      Some((colNames, filteredDF))
    } else None
  }
}

object SimilarityIndex {

  case class SimilarityIndexParams(bucketLen: Double = 3, minDF: Int = 2,
                                   binaryMode: Boolean = false, threshold: Double = 2.5,
                                   numPercentiles: Int = 10)

}