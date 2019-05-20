import $ivy.`org.apache.spark::spark-core:2.4.3`
import $ivy.`org.apache.spark::spark-sql:2.4.3`
import org.apache.spark._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel

import $file.loaders
import $file.similarities

@main
def main(output: String = "targets_synonyms/",
         modelOutput: String = "targets_synonyms_model/",
         input: String = "assocs_by_diseases/",
         genesFilename: String = "../19.02_gene-data.json",
         numSynonyms: Int = 500): Unit = {
  println(s"running from input: $input to output:$output with synonyms=$numSynonyms")

  val sparkConf = new SparkConf()
    .setAppName("similarities-targets-model")
    .setMaster("local[*]")

  implicit val ss = SparkSession.builder
    .config(sparkConf)
    .getOrCreate

  val genes = loaders.Loaders.loadGenes(genesFilename).cache()
  val gruppedTargets = ss.read.json(input)

  val params = similarities.SimilarityIndexParams(windowSize = 300)
  val algo = new similarities.SimilarityIndex(params)

  val objectModel =
    algo.fit(gruppedTargets, "targets")

  objectModel.saveTo(modelOutput)

  val syns = objectModel.findSynonyms(numSynonyms)(genes, "symbol", "synonyms")
    .write.json(output)
}