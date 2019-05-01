import $ivy.`org.apache.spark::spark-core:2.4.1`
import $ivy.`org.apache.spark::spark-sql:2.4.1`
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object Loaders {
  /** Load efo data from efo index dump so this allows us
    * to navegate through the ontology
    */
  def loadEFO(path: String)(implicit ss: SparkSession): DataFrame = {
    val efoCols = Seq("id", "label", "path_code", "therapeutic_label")
    val stripEfoID = udf((code: String) => code.split("/").last)
    val efos = ss.read.json(path)
      .withColumn("id", stripEfoID(col("code")))
      .drop("code")
      .withColumn("therapeutic_label", explode(col("therapeutic_labels")))
      .withColumn("_paths", explode(col("path_codes")))
      .withColumn("path_code", explode(col("_paths")))
      .select(efoCols.map(col):_*)

    efos
  }

  /** Load gene data from gene index dump in order to have a comprehensive list
    * of genes with their symbol biotype and name
    */
  def loadGenes(path: String)(implicit ss: SparkSession): DataFrame = {
    val geneCols = Seq("id", "biotype", "approved_symbol", "approved_name", "go")
    val genes = ss.read.json(path)
      .select(geneCols.map(col):_*)
      .withColumnRenamed("approved_symbol", "symbol")
      .withColumnRenamed("approved_name", "name")

    genes
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
      .where(col("is_direct") === true)
      .withColumn("score", col("harmonic-sum.overall"))
      .withColumn("target_id", col("target.id"))
      .withColumn("disease_id", col("disease.id"))
      .withColumn("target_name", col("target.gene_info.symbol"))
      .withColumn("disease_name", col("disease.efo_info.label"))
      .select(assocCols.map(col):_*)

    assocs
  }

  /** load go paths generated with scripts/grom_go_to_jsonl.py
    *
    */
  def loadGOPaths(path: String)(implicit ss: SparkSession): DataFrame = {
    val gosCols = Seq("go_id", "go_set")
    val gos = ss.read.json(path)
      .select(gosCols.map(col):_*)

    gos
  }
}
