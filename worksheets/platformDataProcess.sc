import $ivy.`com.github.pathikrit::better-files:3.8.0`
import $ivy.`org.apache.spark::spark-core:2.4.3`
import $ivy.`org.apache.spark::spark-sql:2.4.3`

import better.files._
import better.files.Dsl._
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel

import $file.platformData

import platformData.DFImplicits._

@main
def main(inputPathPrefix: String, outputPathPrefix: String): Unit = {
  val sparkConf = new SparkConf()
    .setAppName("similarities-loaders")
    .setMaster("local[*]")

  implicit val ss = SparkSession.builder
    .config(sparkConf)
    .getOrCreate

  val genes = platformData.Loaders.loadGenes(inputPathPrefix + "19.04_gene-data.json")
    .flattenDataframe()
    .fixColumnNames()
    .persist
  genes.write.json(outputPathPrefix + "targets/")

  val diseases = platformData.Loaders.loadEFO(inputPathPrefix + "19.04_efo-data.json")
    .flattenDataframe()
    .fixColumnNames()
    .persist
  diseases.write.json(outputPathPrefix + "diseases/")

//  val expression = Loaders.loadExpression(inputPathPrefix + "19.04_expression-data.json")
//  expression.write.parquet(outputPathPrefix + "expression/")

  val evidences = platformData.Loaders.loadEvidences(inputPathPrefix + "19.04_evidence-data.json")
    .flattenDataframe()
    .fixColumnNames()
    .repartitionByRange(col("target_id"))
    .sortWithinPartitions(col("target_id"), col("disease_id"))


  val associations = platformData.Loaders.loadAssociations(inputPathPrefix + "19.04_association-data.json")
    .flattenDataframe()
    .fixColumnNames()
    .repartitionByRange(col("target_id"))
    .sortWithinPartitions(col("target_id"), col("disease_id"))

  val assocsEvs = associations
    .join(evidences, Seq("target_id", "disease_id"), "inner")

  val assocsEvsGenes = assocsEvs
    .join(genes, Seq("target_id"), "inner")

  val assocsEvsGenesEfos = assocsEvsGenes
    .join(diseases, Seq("disease_id"), "inner")
    .flattenDataframe()
    .fixColumnNames()

  assocsEvsGenesEfos
    .write
    .json(outputPathPrefix + "ot_data/")

  platformData.Functions.saveSchemaTo(assocsEvsGenesEfos, outputPathPrefix / "schema.json")
}
