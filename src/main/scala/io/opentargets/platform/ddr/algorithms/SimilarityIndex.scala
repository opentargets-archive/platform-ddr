package io.opentargets.platform.ddr.algorithms

import io.opentargets.platform.ddr.algorithms.SimilarityIndex.SimilarityIndexParams
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{BucketedRandomProjectionLSH, CountVectorizer, IDF, VectorAssembler}
import org.apache.spark.sql.functions.{collect_list, column}
import org.apache.spark.sql.DataFrame


class SimilarityIndex(val df: DataFrame, val params: SimilarityIndexParams) {
  // here the algo
  def run(groupBy: String, aggBy: Seq[String]): Option[DataFrame] = {
    aggregateDF(df, groupBy, aggBy) map { x =>

      val cv: CountVectorizer = new CountVectorizer()
        .setInputCol(aggBy.head)
        .setOutputCol("tf")
        .setMinDF(params.minDF)
        .setBinary(params.binaryMode)

      val idf = new IDF()
        .setInputCol("tf")
        .setOutputCol("tf_idf")

      // TODO compute median of the score per vector and median counts to include into the assembler

      //      // Seq("disease_id", "disease_label", "score", "count")
      //      val assembler = new VectorAssembler()
      //        .setInputCols(Array("tf_idf", aggBy.drop(2).head))
      //        .setOutputCol("features")

      val brp = new BucketedRandomProjectionLSH()
        .setBucketLength(params.bucketLen)
        // .setNumHashTables(3)
        .setInputCol("tf_idf")
        .setOutputCol("hashes")

      val pipeline = new Pipeline()
        .setStages(Array(cv, idf /*, assembler, */))

      val tx = pipeline.fit(x)
        .transform(x)

      val brp_model = brp.fit(tx)

      val ttdf = brp_model.transform(tx)

      val r = brp_model.approxSimilarityJoin(ttdf, ttdf, params.threshold).persist()

      r.show(false)
      r.toDF()
    }
  }

  private[ddr] def aggregateDF(df: DataFrame, groupBy: String, aggBy: Seq[String]): Option[DataFrame] =
    aggBy match {
      case x :: xs =>
        val aggL = aggBy.map(x => collect_list(x).as(x))
        val filteredDF = df.groupBy(column(groupBy))
          .agg(aggL.head, aggL.tail: _*)
        Some(filteredDF)
      case Nil => None
    }
}

object SimilarityIndex {

  case class SimilarityIndexParams(bucketLen: Double = 10, minDF: Int = 2,
                                   binaryMode: Boolean = false, threshold: Double = 1.5)

}