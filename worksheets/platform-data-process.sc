import $ivy.`org.apache.spark::spark-core:2.4.3`
import $ivy.`org.apache.spark::spark-sql:2.4.3`
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel

object Loaders {
  /** Load efo data from efo index dump so this allows us
    * to navegate through the ontology
    */
  def loadEFO(path: String)(implicit ss: SparkSession): DataFrame = {
    val efoCols = Seq("id", "label", "path_code", "therapeutic_label")

    val genAncestors = udf((codes: Seq[Seq[String]]) =>
      codes.view.flatten.toSet.toSeq)
    val stripEfoID = udf((code: String) => code.split("/").last)
    val efos = ss.read.json(path)
      .withColumn("id", stripEfoID(col("code")))
      .withColumn("therapeutic_label", explode(col("therapeutic_labels")))
      .withColumn("path_code", genAncestors(col("path_codes")))

    efos
      .repartitionByRange(col("id"))
      .sortWithinPartitions(col("id"))
  }

  /** Load gene data from gene index dump in order to have a comprehensive list
    * of genes with their symbol biotype and name
    */
  def loadGenes(path: String)(implicit ss: SparkSession): DataFrame = {
    val geneCols = Seq("id", "biotype", "approved_symbol", "approved_name", "go")
    val genes = ss.read.json(path)

    genes
      .drop("drugbank", "uniprot", "pfam", "reactome")
      .repartitionByRange(col("id"))
      .sortWithinPartitions(col("id"))
  }

  /** Load expression data index dump and exploding the tissues vector so
    * having a tissue per row per gene id and mapping each tissue
    * estructure to multi column
    */
  def loadExpression(path: String)(implicit ss: SparkSession): DataFrame = {
    val tissueCols = Seq("id", "_tissue.*")
    val tissues = ss.read.json(path)
      .withColumn("_tissue", explode(col("tissues")))
      .withColumnRenamed("gene", "id")
      .select(tissueCols.map(col):_*)

    tissues
      .repartitionByRange(col("id"))
      .sortWithinPartitions(col("id"))
  }

  /** Load associations from ES index dump and filter by
    * - is_direct == True
    * - and then generate some columns as score target id and name and for diseases
    * get disease id and disease name
    * - cache list unique diseases and list unique targets
    */
  def loadAssociations(path: String)(implicit ss: SparkSession): DataFrame = {
    val assocCols = Seq("score", "target_id", "disease_id", "target_name", "disease_name")
    val assocs = ss.read.json(path)
      .withColumn("score", col("harmonic-sum.overall"))
      .withColumn("target_id", col("target.id"))
      .withColumn("disease_id", to_json(col("disease.id")))
      .withColumn("target_name", col("target.gene_info.symbol"))
      .withColumn("disease_name", col("disease.efo_info.label"))
    assocs
  }

  def loadEvidences(path: String)(implicit ss: SparkSession): DataFrame = {
    val evidences = ss.read.json(path)
    evidences
  }
}

@main
def main(inputPathPrefix: String = "./", outputPathPrefix: String = "out/"): Unit = {
  val sparkConf = new SparkConf()
    .setAppName("similarities-loaders")
    .setMaster("local[*]")

  implicit val ss = SparkSession.builder
    .config(sparkConf)
    .getOrCreate

  val genes = Loaders.loadGenes(inputPathPrefix + "19.04_gene-data.json")
  genes.write.parquet(outputPathPrefix + "targets/")

  val diseases = Loaders.loadEFO(inputPathPrefix + "19.04_efo-data.json")
  diseases.write.parquet(outputPathPrefix + "diseases/")

  val expression = Loaders.loadExpression(inputPathPrefix + "19.04_expression-data.json")
  expression.write.parquet(outputPathPrefix + "expression/")

  val evidences = Loaders.loadEvidences(inputPathPrefix + "19.04_evidence-data.json")
  evidences
    .repartitionByRange(col("target.id"))
    .sortWithinPartitions(col("target.id"), col("disease.id"))
    .write.parquet(outputPathPrefix + "evidences_t")

  evidences
    .repartitionByRange(col("disease.id"))
    .sortWithinPartitions(col("disease.id"), col("target.id"))
    .write.parquet(outputPathPrefix + "evidences_d")

  val associations = Loaders.loadAssociations(inputPathPrefix + "19.04_association-data.json")
  associations
    .repartitionByRange(col("target.id"))
    .sortWithinPartitions(col("target.id"), col("disease.id"))
    .write.parquet(outputPathPrefix + "associations_t")

  associations
    .repartitionByRange(col("disease.id"))
    .sortWithinPartitions(col("disease.id"), col("target.id"))
    .write.parquet(outputPathPrefix + "associations_d")
}
