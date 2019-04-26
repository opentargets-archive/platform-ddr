import $ivy.`org.apache.spark::spark-core:2.4.0`
import $ivy.`org.apache.spark::spark-sql:2.4.0`
import org.apache.spark._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.storage.StorageLevel

object Loaders {
  /** Load efo data from efo index dump so this allows us
    * to navegate through the ontology
    */
  def loadEFO(path: String)(implicit ss: SparkSession): DataFrame = {
    val stripEfoID = udf((code: String) => code.split("/").last)
    val efos = ss.read.json(path)
      .withColumn("id", stripEfoID(col("code")))

    efos
  }

  /** Load gene data from gene index dump in order to have a comprehensive list
    * of genes with their symbol biotype and name
    */
  def loadGenes(path: String)(implicit ss: SparkSession): DataFrame = {
    val geneCols = Seq("id", "biotype", "approved_symbol", "approved_name")
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
    val assocCols = Seq("score", "target_id", "disease_id")
    val assocs = ss.read.json(path)
      .where(col("is_direct") === true)
      .withColumn("score", col("harmonic-sum.overall"))
      .withColumn("target_id", col("target.id"))
      .withColumn("disease_id", col("disease.id"))
//      .withColumn("target_name", col("target.gene_info.symbol"))
//      .withColumn("disease_name", col("disease.efo_info.label"))
      .select(assocCols.map(col):_*)

    assocs
  }
}

@main
def main(output: String = "./", pval: Double = 0.05): Unit = {
  println(s"running to $output with pval=$pval")

  val sparkConf = new SparkConf()
    .setAppName("similarities-targets")
    .setMaster("local[*]")

  implicit val ss = SparkSession.builder
    .config(sparkConf)
    .getOrCreate

  import ss.implicits._

  val genes = Loaders.loadGenes("../19.02_gene-data.json")
  val tissues = Loaders.loadExpression("../19.02_expression-data.json")

  /*
    left outer join data genes with tissues so expecting genes
    with no tissue data
   */
  val gDF = genes.join(tissues, Seq("id"), "left_outer")
    .orderBy(col("id").asc)
    .persist(StorageLevel.DISK_ONLY)

  val eDF = Loaders.loadEFO("../19.02_efo-data.json")
    .persist(StorageLevel.DISK_ONLY)

  val aDF = Loaders.loadAssociations("../19.02_association-data.json")
    .where(col("score") >= 0.1)
    .persist(StorageLevel.DISK_ONLY)

  // get list of uniq targets and diseases from filtered associations
  val assocsG = aDF.select("target_id", "target_name").distinct.cache()
  val assocsD = aDF.select("disease_id", "disease_name").distinct.cache()

  gDF.show(false)
  eDF.show(false)
  aDF.show(false)
  assocsG.show(false)
  assocsD.show(false)

}

// load gene index from ES dump
// load gene expression for each gene from index
// get `P` (Biological Process) starting terms 'P:axon guidance'
// for each P get the inferred tree up to the root
// combine them from top to botton
// remove sets / |S| == 1 and |S| >