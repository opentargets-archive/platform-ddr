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
      .drop("code")
      .withColumn("therapeutic_label", explode(col("therapeutic_labels")))
      .withColumn("path_code", explode(genAncestors(col("path_codes"))))
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

  def loadEvidences(path: String)(implicit ss: SparkSession): DataFrame = {
    val evidences = ss.read.json(path)
    evidences
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

  /** load string-db datasets using the mappings and the COG data
    * it needs the protein mapping to symbols gene ids
    */
  def loadStringDB(protLinks: String, protInfo: String)(implicit ss: SparkSession): DataFrame = {
    val prot2Name = ss
      .read
      .option("sep", "\t")
      .option("mode", "DROPMALFORMED")
      .csv(protInfo)
      .toDF("pid", "symbol", "protein_size", "annotation")
      .select("pid", "symbol")
      .filter(not(col("symbol").startsWith(lit("ENSG"))))
      .filter(not(col("symbol").startsWith(lit("HGNC:"))))
      .cache()

    val p2p = ss
      .read
      .option("sep", " ")
      .option("mode", "DROPMALFORMED")
      .csv(protLinks)
      .toDF("protein1", "protein2", "neighborhood", "fusion", "cooccurence", "coexpression",
        "experimental", "database", "textmining", "combined_score")
      .where(col("coexpression") > 0 and col("combined_score") > 700)

    val links = p2p.join(prot2Name,
      col("protein1") === col("pid"),
      "inner")
        .drop("pid", "protein1")
        .withColumnRenamed("symbol", "symbol_a")
        .join(prot2Name,
          col("protein2") === col("pid"),
          "inner")
        .withColumnRenamed("symbol", "symbol_b")
        .drop("protein2", "pid")
        .groupBy("symbol_a")
        .agg(collect_set(col("symbol_b")).as("_stringdb_set"))
        .withColumn("stringdb_set",array_union(array(col("symbol_a")), col("_stringdb_set")))
        .drop("_stringdb_set")

    links
  }
}

@main
def main(): Unit = {
  val sparkConf = new SparkConf()
    .setAppName("similarities-loaders")
    .setMaster("local[*]")

  implicit val ss = SparkSession.builder
    .config(sparkConf)
    .getOrCreate

  val ddf = Loaders.loadStringDB("../9606.protein.links.detailed.v11.0.txt", "../9606.protein.info.v11.0.txt")
  ddf.write.json("targets_stringdb/")

//  val ddf = Loaders.loadEvidences("../19.04_evidence-data.json")
//  ddf.printSchema
}
