import $file.platformData
import $ivy.`com.github.pathikrit::better-files:3.8.0`
import $ivy.`com.typesafe:config:1.3.4`
import $ivy.`org.apache.spark::spark-core:2.4.3`
import $ivy.`org.apache.spark::spark-mllib:2.4.3`
import $ivy.`org.apache.spark::spark-sql:2.4.3`
import better.files._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import platformData.DFImplicits._
import platformData.{Functions, Loaders}

@main
def main(inputPathPrefix: String, outputPathPrefix: String): Unit = {
  val sparkConf = new SparkConf()
    .set("spark.driver.maxResultSize", "0")
    .setAppName("similarities-loaders")
    .setMaster("local[*]")

  implicit val ss = SparkSession.builder
    .config(sparkConf)
    .getOrCreate

  val geneDF = Loaders.loadGenes(inputPathPrefix + "19.04_gene-data.json")
  geneDF.printSchema()

  val genes = geneDF
    .flattenDataframe()
    .fixColumnNames()

  genes.printSchema()

  genes.write.json(outputPathPrefix + "targets/")
  Functions.saveSchemaTo(genes, outputPathPrefix / "targets" / "schema.json",
    outputPathPrefix / "targets" / "schema.sql", "ot.targets")
  genes.printSchema()

  val diseases = Loaders.loadEFO(inputPathPrefix + "19.04_efo-data.json")
    .flattenDataframe()
    .fixColumnNames()
  diseases.write.json(outputPathPrefix + "diseases/")
  Functions.saveSchemaTo(diseases, outputPathPrefix / "diseases" / "schema.json",
    outputPathPrefix / "diseases" / "schema.sql", "ot.diseases")
  diseases.printSchema()

  val expression = Loaders.loadExpression(inputPathPrefix + "19.04_expression-data.json")
    .flattenDataframe()
    .fixColumnNames()
  expression.write.json(outputPathPrefix + "expression/")
  Functions.saveSchemaTo(expression, outputPathPrefix / "expression" / "schema.json",
    outputPathPrefix / "expression" / "schema.sql", "ot.expression")
  expression.printSchema()

  val evidences = Loaders.loadEvidences(inputPathPrefix + "19.04_evidence-data.json")
    .flattenDataframe()
    .fixColumnNames()
    .repartitionByRange(col("target_id"))
    .sortWithinPartitions(col("target_id"), col("disease_id"))

  val evidencesWithGenesEfos = evidences
    .join(genes, Seq("target_id"), "inner")
    .repartitionByRange(col("disease_id"))
    .sortWithinPartitions(col("disease_id"), col("target_id"))
    .join(diseases, Seq("disease_id"), "inner")

  evidencesWithGenesEfos.write.json(outputPathPrefix + "evidences/")
  Functions.saveSchemaTo(evidencesWithGenesEfos, outputPathPrefix / "evidences" / "schema.json",
    outputPathPrefix / "evidences" / "schema.sql", "ot.evidences")
  evidencesWithGenesEfos.printSchema()

  val associations = Loaders.loadAssociations(inputPathPrefix + "19.04_association-data.json")
    .flattenDataframe()
    .fixColumnNames()
    .repartitionByRange(col("target_id"))
    .sortWithinPartitions(col("target_id"), col("disease_id"))

  val aggEvidences = evidences.groupBy(col("target_id"), col("disease_id"))
    .agg(collect_list(col("datasource")).as("evs_datasources"),
      collect_list(col("scores__association_score")).as("evs_scores"))

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

  Functions.saveSchemaTo(assocsEvsGenesEfos, outputPathPrefix / "associations" / "schema.json",
    outputPathPrefix / "associations" / "schema.json", "ot.associations")
  assocsEvsGenesEfos.printSchema()
}
