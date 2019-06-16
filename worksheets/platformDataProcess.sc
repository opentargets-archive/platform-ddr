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
import platformData.Functions
import platformData.Loaders

@main
def main(inputPathPrefix: String, outputPathPrefix: String): Unit = {
  val sparkConf = new SparkConf()
    .setAppName("similarities-loaders")
    .setMaster("local[*]")

  implicit val ss = SparkSession.builder
    .config(sparkConf)
    .getOrCreate

  val genes = Loaders.loadGenes(inputPathPrefix + "19.04_gene-data.json")
    .flattenDataframe()
    .fixColumnNames()
    .persist
  genes.write.json(outputPathPrefix + "targets/")
  Functions.saveSchemaTo(genes, outputPathPrefix / "targets" / "schema.json")

  val diseases = Loaders.loadEFO(inputPathPrefix + "19.04_efo-data.json")
    .flattenDataframe()
    .fixColumnNames()
    .persist
  diseases.write.json(outputPathPrefix + "diseases/")
  Functions.saveSchemaTo(diseases, outputPathPrefix / "diseases" / "schema.json")

//  val expression = Loaders.loadExpression(inputPathPrefix + "19.04_expression-data.json")
//  expression.write.parquet(outputPathPrefix + "expression/")

  val evidences = Loaders.loadEvidences(inputPathPrefix + "19.04_evidence-data.json")
    .flattenDataframe()
    .fixColumnNames()
    .repartitionByRange(col("target_id"))
    .sortWithinPartitions(col("target_id"), col("disease_id"))
    .persist

  val evidencesWithGenesEfos = evidences
    .join(genes, Seq("target_id"), "inner")
    .join(diseases, Seq("disease_id"), "inner")

  evidencesWithGenesEfos.write.json(outputPathPrefix + "evidences/")
  Functions.saveSchemaTo(evidencesWithGenesEfos, outputPathPrefix / "evidences" / "schema.json")

  val associations = Loaders.loadAssociations(inputPathPrefix + "19.04_association-data.json")
    .flattenDataframe()
    .fixColumnNames()
    .repartitionByRange(col("target_id"))
    .sortWithinPartitions(col("target_id"), col("disease_id"))

  val aggEvidences = evidences.groupBy(col("target_id"), col("disease_id"))
    .agg(collect_list(col("datasource")).as("evs_datasources"),
      collect_list(col("scores$association_score")).as("evs_scores"))

  val assocsEvs = associations
    .join(aggEvidences, Seq("target_id", "disease_id"), "inner")

  val assocsEvsGenes = assocsEvs
    .join(genes, Seq("target_id"), "inner")

  val assocsEvsGenesEfos = assocsEvsGenes
    .join(diseases, Seq("disease_id"), "inner")
    .flattenDataframe()
    .fixColumnNames()

  assocsEvsGenesEfos
    .write
    .json(outputPathPrefix + "associations/")

  Functions.saveSchemaTo(assocsEvsGenesEfos, outputPathPrefix / "associations" / "schema.json")
}