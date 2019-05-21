import $ivy.`org.apache.spark::spark-core:2.4.3`
import $ivy.`org.apache.spark::spark-sql:2.4.3`
import org.apache.spark._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import $file.loaders
import $file.funcs
import org.apache.spark.storage.StorageLevel

def buildGroupByDisease(zscoreLevel: Int, rnaLevel: Int, proteinLevel: Int)(implicit ss: SparkSession): DataFrame = {
  val bannedDiseases = List("EFO_0000311", "EFO_0000616", "EFO_0000313")
  val genes = loaders.Loaders.loadGenes("../19.04_gene-data.json")
  val tissues = loaders.Loaders.loadExpression("../19.04_expression-data.json")
  val ddf = loaders.Loaders.loadStringDB("../9606.protein.links.detailed.v11.0.txt",
    "../9606.protein.info.v11.0.txt")
    .repartitionByRange(col("symbol_a"))
    .orderBy(col("symbol_a"))
    .cache()

  /* load gene index from ES dump
     load gene expression for each gene from index
     get `P` (Biological Process) starting terms 'P:axon guidance'
     for each P get the inferred ancestors
     we should stop 3 steps before http://ols.wordvis.com/q=GO:0008150 (biological process)
     GO:0008150
     combine them from bottom to top
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

  /*
    load associations but only direct ones and filter all with score > 0.1
   */
  val assocs = loaders.Loaders.loadAssociations("../19.04_association-data.json")
    .where(col("score") > 0.1)
    .repartitionByRange(col("target_id"))
    .where(not(col("disease_id") isInCollection bannedDiseases))

  val aCols = Seq("target_id", "target_name", "disease_id", "go_id", "go_term", "label")

  /*
    join assocs with genes where there is a biological process and either zscore >= 3 or
    rna level >= 5?
    group them by organ and then
    join with stringdb coexpressed links
   */
  val aDF = assocs
    .join(gDF, assocs("target_id") === gDF("id"), "inner")
    .where((col("go_bp") === true) and
      ((col("rna.zscore") >= zscoreLevel) or
        (col("protein.level") >= proteinLevel) or
        (col("rna.level") >= rnaLevel)))
    .select(aCols.map(col):_*)
    .join(ddf, col("target_name") === col("symbol_a"), "left_outer")
    .repartitionByRange(col("disease_id"), col("label"))

  // load go ontology and cache them in order to join with joint assocs
  val goPaths = loaders.Loaders.loadGOPaths("../go_paths.json").drop("go_term", "go_paths")
    .orderBy(col("go_id"))
    .cache()

  /*
    assocs inner join gopaths, group by go element and aggregate set of targets and list of stringdb sets and counts
    then flatten stringdb target list of sets and compute some counts
   */
  val computedSets = aDF
    .join(goPaths, Seq("go_id"), "inner")
    .withColumn("go_path_elem", explode(slice(col("go_set"), 1, 2)))
    .groupBy(col("disease_id"), col("label"), col("go_path_elem"))
    .agg(first(col("go_term")).as("go_term"),
      collect_set(col("target_name")).as("targets"),
      collect_set(col("stringdb_set")).as("stringdb_set_set"))
    .withColumn("targets_count", size(col("targets")))
    // we dont want sets of cardinality 1
    .where(col("targets_count") > 1)
    .withColumn("targets_joint",
      array_union(col("targets"),
        funcs.Functions.getDuplicates(flatten(col("stringdb_set_set")))))
    .withColumn("targets_joint_counts", size(col("targets_joint")))
    .drop("stringdb_set_set")

  computedSets
}

@main
def main(output: String = "assocs_by_diseases/",
         rnaLevel: Int = 5,
         zscoreLevel: Int = 3,
         proteinLevel: Int = 1): Unit = {
  println(s"running to $output with >= level=$rnaLevel , zscore=$zscoreLevel and protein >= level=$proteinLevel")

  val sparkConf = new SparkConf()
    .setAppName("similarities-targets")
    .setMaster("local[*]")

  implicit val ss = SparkSession.builder
    .config(sparkConf)
    .getOrCreate

  val ddf = buildGroupByDisease(zscoreLevel, rnaLevel, proteinLevel)
  ddf.write.json(output)
}
