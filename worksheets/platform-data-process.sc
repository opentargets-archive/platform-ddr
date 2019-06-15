import $ivy.`com.github.pathikrit::better-files:3.8.0`
import $ivy.`org.apache.spark::spark-core:2.4.3`
import $ivy.`org.apache.spark::spark-sql:2.4.3`

import better.files._
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel

object Loaders {
  /** Load efo data from efo index dump so this allows us
    * to navegate through the ontology
    */
  def loadEFO(path: String)(implicit ss: SparkSession): DataFrame = {
    val genAncestors = udf((codes: Seq[Seq[String]]) =>
      codes.view.flatten.toSet.toSeq)

    val stripEfoID = udf((code: String) => code.split("/").last)
    val efos = ss.read.json(path)
      .withColumn("disease_id", stripEfoID(col("code")))
      .withColumn("path_code", genAncestors(col("path_codes")))
      .drop("paths")

    efos
      .repartitionByRange(col("disease_id"))
      .sortWithinPartitions(col("disease_id"))
  }

  /** Load gene data from gene index dump in order to have a comprehensive list
    * of genes with their symbol biotype and name
    */
  def loadGenes(path: String)(implicit ss: SparkSession): DataFrame = {
    val genes = ss.read.json(path)

    genes
      .withColumnRenamed("id", "target_id")
      .repartitionByRange(col("target_id"))
      .sortWithinPartitions(col("target_id"))
      .selectExpr("*", "_private.facets.*", "tractability.*")
      .drop("drugbank", "uniprot", "pfam", "reactome", "_private", "ortholog", "tractability")
  }

  /** Load expression data index dump and exploding the tissues vector so
    * having a tissue per row per gene id and mapping each tissue
    * estructure to multi column
    */
  def loadExpression(path: String)(implicit ss: SparkSession): DataFrame = {
    // val tissueCols = Seq("id", "_tissue.*")
    val tissues = ss.read.json(path)
      // .withColumn("_tissue", explode(col("tissues")))
      .withColumnRenamed("gene", "target_id")

    tissues
      .repartitionByRange(col("target_id"))
      .sortWithinPartitions(col("target_id"))
  }

  /** Load associations from ES index dump and filter by
    * - is_direct == True
    * - and then generate some columns as score target id and name and for diseases
    * get disease id and disease name
    * - cache list unique diseases and list unique targets
    */
  def loadAssociations(path: String)(implicit ss: SparkSession): DataFrame = {
    val assocs = ss.read.json(path)
      .withColumn("score", col("harmonic-sum.overall"))
      .withColumn("target_id", col("target.id"))
      .withColumn("disease_id", col("disease.id"))
      .withColumn("target_name", col("target.gene_info.symbol"))
      .withColumn("disease_name", col("disease.efo_info.label"))
      .withColumn("score_datasource", col("harmonic-sum.datasources"))
      .withColumn("score_datatype", col("harmonic-sum.datatypes"))
      .drop("private", "_private", "target", "disease", "id")
    assocs
  }

  def loadEvidences(path: String)(implicit ss: SparkSession): DataFrame = {
    val evidences = ss.read.json(path)
    evidences
      .drop("private", "validated_against_schema_version")
      .selectExpr("target.id as target_id", "disease.id as disease_id",
        "literature.references as evs_literature_ref", "scores.association_score as evs_score",
        "unique_association_fields as evs_unique_field")
      .groupBy(col("target_id"), col("disease_id"))
      .agg(collect_list(col("evs_literature_ref")).as("evs_literature_refs"),
        collect_list(col("evs_score")).as("evs_scores"),
        collect_list(col("evs_unique_field")).as("evs_unique_fields"))
  }
}

@main
def main(inputPathPrefix: String, outputPathPrefix: String): Unit = {
  val sparkConf = new SparkConf()
    .setAppName("similarities-loaders")
    .setMaster("local[*]")

  implicit val ss = SparkSession.builder
    .config(sparkConf)
    .getOrCreate

  val genes = Loaders.loadGenes(inputPathPrefix + "19.04_gene-data.json")
  genes.write.json(outputPathPrefix + "targets/")

  val diseases = Loaders.loadEFO(inputPathPrefix + "19.04_efo-data.json")
  diseases.write.json(outputPathPrefix + "diseases/")

//  val expression = Loaders.loadExpression(inputPathPrefix + "19.04_expression-data.json")
//  expression.write.parquet(outputPathPrefix + "expression/")

  val evidences = Loaders.loadEvidences(inputPathPrefix + "19.04_evidence-data.json")
    .repartitionByRange(col("target_id"))
    .sortWithinPartitions(col("target_id"), col("disease_id"))

  val associations = Loaders.loadAssociations(inputPathPrefix + "19.04_association-data.json")
    .repartitionByRange(col("target_id"))
    .sortWithinPartitions(col("target_id"), col("disease_id"))

  val assocsEvs = associations
    .join(evidences, Seq("target_id", "disease_id"), "inner")

  val assocsEvsGenes = assocsEvs
    .join(genes, Seq("target_id"), "inner")

  val assocsEvsGenesEfos = assocsEvsGenes
    .join(diseases, Seq("disease_id"), "inner")

  assocsEvsGenesEfos.write.json(outputPathPrefix + "ot_data/")

  outputPathPrefix / "schema.json" < assocsEvsGenesEfos.schema.json
}
