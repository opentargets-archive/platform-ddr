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
import org.apache.spark.storage.StorageLevel
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

  val drugList = ss.read
    .format("csv")
    .option("header", "false")
    .option("inferSchema", "true")
    .option("delimiter","\t")
    .option("ignoreLeadingWhiteSpace", "true")
    .option("ignoreTrailingWhiteSpace", "true")
    .option("mode", "DROPMALFORMED")
    .csv("drug_names.txt")
    .toDF("_drug_name")
    .withColumn("drug_name", lower(col("_drug_name")))
    .drop("_drug_name")
    .orderBy(col("drug_name"))
    .cache()

  drugList.show(false)

  val lines = Loaders.loadFDA(inputPathPrefix)
//    .sample(0.01)

  //safetyreportid
  //serious
  //receivedate
  //primarysourcecountry
  //primarysource.qualification as qualification
  //patient.reaction[].reactionmeddrapt
  //patient.drug[].medicinalproduct
  //patient.drug[].openfda.generic_name[]
  //patient.drug[].openfda.substance_name[]
  //patient.drug[].activesubstance.activesubstancename
  //patient.drug[].drugcharacterization

  val fdas = lines.withColumn("reaction", explode(col("patient.reaction")))
    .withColumn("drug", explode(col("patient.drug")))
    .selectExpr("safetyreportid", "serious", "receivedate", "primarysourcecountry", "qualification", "reaction.reactionmeddrapt as reaction_reactionmeddrapt" ,
      "drug.medicinalproduct as drug_medicinalproduct", "drug.openfda.generic_name as drug_generic_name_list", "drug.openfda.substance_name as drug_substance_name_list",
      "drug.drugcharacterization as drugcharacterization")
    .drop("patient", "reaction", "drug")
    .where(col("qualification").isInCollection(Seq("1", "2", "3")) and col("drugcharacterization") === "1")
    .withColumn("drug_names", array_distinct(array_union(array(col("drug_medicinalproduct")),
      array_union(col("drug_generic_name_list"), col("drug_substance_name_list")))))
    .withColumn("drug_name", lower(explode(col("drug_names"))))
    .drop("drug_medicinalproduct", "drug_generic_name_list", "drug_substance_name_list")
    .persist(StorageLevel.DISK_ONLY)

  val aggByReactions = fdas.groupBy(col("reaction_reactionmeddrapt"))
    .agg(countDistinct(col("safetyreportid")).as("uniq_report_ids_by_reaction"))

  val aggByDrugs = fdas.groupBy(col("drug_name"))
    .agg(countDistinct(col("safetyreportid")).as("uniq_report_ids_by_drug"))

  val uniqReports = fdas.select("safetyreportid").distinct.count

  val doubleAgg = fdas.groupBy(col("drug_name"), col("reaction_reactionmeddrapt"))
    .agg(countDistinct(col("safetyreportid")).as("uniq_report_ids"))
    .join(aggByDrugs, Seq("drug_name"), "inner")
    .join(aggByReactions, Seq("reaction_reactionmeddrapt"), "inner")
    .withColumn("C", col("uniq_report_ids_by_drug") - col("uniq_report_ids"))
    .withColumn("B", col("uniq_report_ids_by_reaction") - col("uniq_report_ids"))
    .withColumn("D", ((lit(uniqReports) - col("uniq_report_ids_by_drug")) - col("uniq_report_ids_by_reaction")) + col("uniq_report_ids"))
    .withColumnRenamed("uniq_report_ids", "A")
    .join(drugList, Seq("drug_name"), "inner")
//    .selectExpr("drug_name", "reaction_reactionmeddrapt", "uniq_report_ids as A", "B", "C", "D")

//  fdas.write.json(outputPathPrefix + "/filtered/")
//  aggByReactions.write.json(outputPathPrefix + "/uniq_report_ids_by_reactions/")
//  aggByDrugs.write.json(outputPathPrefix + "/uniq_report_ids_by_reactions/")
  doubleAgg.write.json(outputPathPrefix + "/agg/")
  println(s"uniq reports $uniqReports")
}
