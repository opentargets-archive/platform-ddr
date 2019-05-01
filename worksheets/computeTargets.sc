import $ivy.`org.apache.spark::spark-core:2.4.1`
import $ivy.`org.apache.spark::spark-sql:2.4.1`
import org.apache.spark._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import $file.loaders
import org.apache.spark.storage.StorageLevel

def buildGroupByDisease(zscoreLevel: Int, proteinLevel: Int)(implicit ss: SparkSession): DataFrame = {
  val genes = loaders.Loaders.loadGenes("../19.02_gene-data.json")
  val tissues = loaders.Loaders.loadExpression("../19.02_expression-data.json")

  /* load gene index from ES dump
     load gene expression for each gene from index
     get `P` (Biological Process) starting terms 'P:axon guidance'
     for each P get the inferred tree up to the root
     we should stop before http://ols.wordvis.com/q=GO:0008150 (biological process)
     GO:0008150
     combine them from top to botton
     remove sets / |S| == 1 and |S| >
   */

  /*
    left outer join data genes with tissues so expecting genes
    with no tissue data
   */
  val checkIfBP = udf((term: String) => term.startsWith("P:"))
  val gDF = genes
    .select("id", "go")
    .withColumn("go_item", explode(col("go")))
    .withColumn("go_id", col("go_item.id"))
    .withColumn("go_term", col("go_item.value.term"))
    .drop("go", "go_item")
    .withColumn("go_bp", checkIfBP(col("go_term")))
    .join(tissues, Seq("id"), "left_outer")
    .repartitionByRange(col("id"))
    .orderBy(col("id"))
    .persist(StorageLevel.DISK_ONLY)

  val assocs = loaders.Loaders.loadAssociations("../19.02_association-data.json")
    .where(col("score") >= 0.1)
    .repartitionByRange(col("disease_id"))
    .orderBy(col("target_id"))
    .persist(StorageLevel.DISK_ONLY)

  val aCols = Seq("target_id", "target_name", "disease_id", "go_id", "go_term", "organ_name")

  val aDF = assocs
    .join(gDF, assocs("target_id") === gDF("id"), "left_outer")
    .where((col("go_bp") === true) and
      (col("rna.zscore") >= zscoreLevel or
        col("protein.level") >= proteinLevel))
    .withColumn("organ_name", explode(col("organs")))
    .select(aCols.map(col):_*)
    .repartitionByRange(col("disease_id"), col("organ_name"))
    .orderBy(col("go_id"))
    .persist(StorageLevel.DISK_ONLY)

  val goPaths = loaders.Loaders.loadGOPaths("../go_paths.json").drop("go_term", "go_paths")
    .orderBy(col("go_id"))
    .cache()

  aDF
    .join(goPaths, Seq("go_id"), "inner")
    .withColumn("go_path_elem", explode(col("go_set")))
    .groupBy(col("disease_id"), col("organ_name"), col("go_path_elem"))
    .agg(first(col("go_term")).as("go_term"),
      collect_set(col("target_name")).as("targets"),
      approx_count_distinct(col("target_name")).as("targets_count"))
    .where(col("targets_count") > 1)
}

@main
def main(output: String = "assocs_by_diseases/",
         zscoreLevel: Int = 3,
         proteinLevel: Int = 1): Unit = {
  println(s"running to $output with >= zscore=$zscoreLevel and protein >= level=$proteinLevel")

  val sparkConf = new SparkConf()
    .setAppName("similarities-targets")
    .setMaster("local[*]")

  implicit val ss = SparkSession.builder
    .config(sparkConf)
    .getOrCreate

  val ddf = buildGroupByDisease(zscoreLevel, proteinLevel)
  ddf.write.json(output)
}
