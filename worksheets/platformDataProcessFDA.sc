import $ivy.`com.github.pathikrit::better-files:3.8.0`
import $ivy.`com.typesafe:config:1.3.4`
import $ivy.`org.apache.spark::spark-core:2.4.3`
import $ivy.`org.apache.spark::spark-mllib:2.4.3`
import $ivy.`org.apache.spark::spark-sql:2.4.3`
import $ivy.`com.github.pathikrit::better-files:3.8.0`

import better.files.Dsl._
import better.files._
import better.files._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

object Loaders {
  def loadDrugList(path: String)(implicit ss: SparkSession): DataFrame = {
    val drugList = ss.read
      .format("csv")
      .option("header", "false")
      .option("inferSchema", "true")
      .option("delimiter","\t")
      .option("ignoreLeadingWhiteSpace", "true")
      .option("ignoreTrailingWhiteSpace", "true")
      .option("mode", "DROPMALFORMED")
      .csv(path)
      .toDF("_drug_name")
      .withColumn("drug_name", lower(col("_drug_name")))
      .drop("_drug_name")
      .orderBy(col("drug_name"))

    drugList
  }

  def loadFDA(path: String)(implicit ss: SparkSession): DataFrame = {
    val fda = ss.read.json(path)

    val columns = Seq("safetyreportid",
      "serious",
      "receivedate",
      "primarysource.reportercountry as primarysourcecountry",
      "primarysource.qualification as qualification",
      "patient")
    fda.selectExpr(columns:_*)
  }
}

@main
def main(inputPathPrefix: String, outputPathPrefix: String): Unit = {
  val sparkConf = new SparkConf()
    .set("spark.driver.maxResultSize", "0")
    .setAppName("similarities-loaders")
    .setMaster("local[*]")

  implicit val ss = SparkSession.builder
    .config(sparkConf)
    .getOrCreate

  import ss.implicits._

  // the curated drug list we want
  val drugList = Loaders.loadDrugList("drug_names.txt").cache()

  // load FDA raw lines
  val lines = Loaders.loadFDA(inputPathPrefix)

  val fdas = lines.withColumn("reaction", explode(col("patient.reaction")))
    // after explode this we will have reaction-drug pairs
    .withColumn("drug", explode(col("patient.drug")))
    // just the fields we want as columns
    .selectExpr("safetyreportid", "serious", "receivedate", "primarysourcecountry",
      "qualification",
      "lower(reaction.reactionmeddrapt) as reaction_reactionmeddrapt" ,
      "lower(drug.medicinalproduct) as drug_medicinalproduct",
      "drug.openfda.generic_name as drug_generic_name_list",
      "drug.openfda.substance_name as drug_substance_name_list",
      "drug.drugcharacterization as drugcharacterization")
    // we dont need these columns anymore
    .drop("patient", "reaction", "drug", "_reaction")
    // delicated filter which should be looked at FDA API to double check
    .where(col("qualification").isInCollection(Seq("1", "2", "3")) and col("drugcharacterization") === "1")
    // drug names comes in a large collection of multiple synonyms but it comes spread across multiple fields
    .withColumn("drug_names", array_distinct(array_union(array(col("drug_medicinalproduct")),
      array_union(col("drug_generic_name_list"), col("drug_substance_name_list")))))
    // the final real drug name
    .withColumn("_drug_name", explode(col("drug_names")))
    .withColumn("drug_name", lower(col("_drug_name")))
    // rubbish out
    .drop("drug_generic_name_list", "drug_substance_name_list", "_drug_name")
    .where($"drug_name".isNotNull and $"reaction_reactionmeddrapt".isNotNull and
      $"safetyreportid".isNotNull)
    // and we will need this processed data later on
    .persist(StorageLevel.DISK_ONLY)

  // save the example data into a folder for further inspection
  fdas.where($"drug_name" === "aspirin" and
    $"reaction_reactionmeddrapt" === "completed suicide")
    .write.json(outputPathPrefix + "/fdas_for_aspirin_completed_suicide/")

  // total unique report ids count grouped by reaction
  val aggByReactions = fdas.groupBy(col("reaction_reactionmeddrapt"))
    .agg(countDistinct(col("safetyreportid")).as("uniq_report_ids_by_reaction"))

  // total unique report ids count grouped by drug name
  val aggByDrugs = fdas.groupBy(col("drug_name"))
    .agg(countDistinct(col("safetyreportid")).as("uniq_report_ids_by_drug"))

  // total unique report ids
  val uniqReports = fdas.select("safetyreportid").distinct.count

  // per drug-reaction pair
  val doubleAgg = fdas.groupBy(col("drug_name"), col("reaction_reactionmeddrapt"))
    .agg(countDistinct(col("safetyreportid")).as("uniq_report_ids"))
    .withColumnRenamed("uniq_report_ids", "A")
    .join(aggByDrugs, Seq("drug_name"), "inner")
    .join(aggByReactions, Seq("reaction_reactionmeddrapt"), "inner")
    .withColumn("C", col("uniq_report_ids_by_drug") - col("A"))
    .withColumn("B", col("uniq_report_ids_by_reaction") - col("A"))
    .withColumn("D", lit(uniqReports) - col("uniq_report_ids_by_drug") - col("uniq_report_ids_by_reaction") + col("A"))
    .withColumn("aterm", $"A" * (log($"A") - log($"A" + $"B")))
    .withColumn("cterm", $"C" * (log($"C") - log($"C" + $"D")))
    .withColumn("acterm", ($"A" + $"C") * (log($"A" + $"C") - log($"A" + $"B" + $"C" + $"D")) )
    .withColumn("llr", $"aterm" + $"cterm" - $"acterm")
    // Max_iae (llr_ij) (all ae for a drug)
    // filter the drugs we want
    .join(drugList, Seq("drug_name"), "inner")

  fdas.write.json(outputPathPrefix + "/fdas/")
  doubleAgg.write.json(outputPathPrefix + "/agg/")
  doubleAgg
    .join(fdas, Seq("drug_name", "reaction_reactionmeddrapt"), "inner")
    .write.json(outputPathPrefix + "/agg_with_fdas/")
}
