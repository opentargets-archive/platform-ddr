import $ivy.`org.apache.spark::spark-core:2.4.1`
import $ivy.`org.apache.spark::spark-sql:2.4.1`
import org.apache.spark._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel

import $file.loaders
import $file.similarities

@main
def main(output: String = "diseases_synonyms/",
         modelOutput: String = "diseases_synonyms_model/",
         input: String = "assocs_by_targets/",
         efosFilename: String = "../19.02_gene-data.json",
         numSynonyms: Int = 500): Unit = {
  println(s"running from input: $input to output:$output with synonyms=$numSynonyms")

  val sparkConf = new SparkConf()
    .setAppName("similarities-targets")
    .setMaster("local[*]")

  implicit val ss = SparkSession.builder
    .config(sparkConf)
    .getOrCreate

  val diseases = loaders.Loaders.loadEFO("../19.02_efo-data.json")
    .select("id", "label").distinct()
    .cache()

  val gruppedDiseases = ss.read.json(input)

  val params = similarities.SimilarityIndexParams()
  val algo = new similarities.SimilarityIndex(params)

  val objectModel =
    algo.fit(gruppedDiseases, "diseases")

  objectModel.saveTo(modelOutput)

  val syns = objectModel.findSynonyms(numSynonyms)(diseases, "label", "synonyms")
    .write.json(output)
}