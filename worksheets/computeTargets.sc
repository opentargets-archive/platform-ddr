import $ivy.`org.apache.spark::spark-core:2.4.0`
import $ivy.`org.apache.spark::spark-sql:2.4.0`
import org.apache.spark._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

import $file.loaders

def buildGroupByDisease(zscoreLevel: Int, proteinLevel: Int)(implicit ss: SparkSession): DataFrame = {
  val genes = loaders.Loaders.loadGenes("../19.02_gene-data.json")
  val tissues = loaders.Loaders.loadExpression("../19.02_expression-data.json")

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

  //  val eDF = Loaders.loadEFO("../19.02_efo-data.json")

  // gDF.write.json(output + "/filtered_genes/")

  val assocs = loaders.Loaders.loadAssociations("../19.02_association-data.json")
    .where(col("score") >= 0.1)
    .repartitionByRange(col("disease_id"))
    .orderBy(col("target_id"))

  val aCols = Seq("target_id", "target_name", "disease_id", "disease_name", "score", "go_id", "go_term", "organ_name")

  val aDF = assocs
    .join(gDF, assocs("target_id") === gDF("id"), "left_outer")
    .where((col("go_bp") === true) and
      (col("rna.zscore") >= zscoreLevel or
        col("protein.level") >= proteinLevel))
    .withColumn("organ_name", explode(col("organs")))
    .select(aCols.map(col):_*)

  //  val assocsG = aDF.select("target_id", "target_name").distinct.cache()
  //  val assocsD = aDF.select("disease_id", "disease_name").distinct.cache()

  //  gDF.show(false)
  //  eDF.show(false)

  aDF.groupBy(col("disease_id"), col("organ_name"))
    .agg(collect_set(col("target_name")).as("targets"),
      collect_set(col("go_term")).as("gos"))
    .where(col("disease_id") === "EFO_0003777")
}

@main
def main(output: String = "./", zscoreLevel: Int = 3, proteinLevel: Int = 1): Unit = {
  println(s"running to $output with >= zscore=$zscoreLevel and protein >= level=$proteinLevel")

  val geneSet = Seq("ENSG00000133703", "ENSG00000115904", "ENSG00000132155")
  val sparkConf = new SparkConf()
    .setAppName("similarities-targets")
    .setMaster("local[*]")

  implicit val ss = SparkSession.builder
    .config(sparkConf)
    .getOrCreate

  val ddf = buildGroupByDisease(zscoreLevel, proteinLevel)
  ddf.write.json(output + "/filtered_assocs_efo_0003777/")
  //  assocsG.show(false)
//  assocsD.show(false)

}

// load gene index from ES dump
// load gene expression for each gene from index
// get `P` (Biological Process) starting terms 'P:axon guidance'
// for each P get the inferred tree up to the root
// combine them from top to botton
// remove sets / |S| == 1 and |S| >