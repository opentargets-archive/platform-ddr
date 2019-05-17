import $ivy.`org.apache.spark::spark-core:2.4.1`
import $ivy.`org.apache.spark::spark-sql:2.4.1`
import org.apache.spark._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import $file.loaders
import org.apache.spark.storage.StorageLevel

/**
  * R has a package to deal with ICD10 mapping
  * https://www.r-bloggers.com/whats-that-disease-called-overview-of-icd-package/
  *
  * symptoms are non curated phenotypes from the complex interactions of diseases
  * and there for some symptoms which are similar across multiple diseases so there
  * must be same sub-mechanisms that cause those symptoms. So if you remove the
  * common ones and use the onriched ones to classify the genes associated with a
  * disease I would be able to link similar targets based of specific symptoms
  * and those targets may be similar ones
 */
def buildGroupByTarget()(implicit ss: SparkSession): DataFrame = {
  val eDF = loaders.Loaders.loadEFO("../19.02_efo-data.json")
    .repartitionByRange(col("id"))
    .orderBy(col("id"))
    .cache()

  val assocs = loaders.Loaders.loadAssociations("../19.02_association-data.json")
    .where(col("score") >= 0.1)
    .repartitionByRange(col("target_id"))
    .orderBy(col("disease_id"))
    .persist(StorageLevel.DISK_ONLY)

  //  val aCols = Seq("target_id", "target_name", "disease_id", "disease_name", "score", "go_id", "go_term", "organ_name")
  val aCols = Seq("target_id", "disease_id", "id", "therapeutic_label", "path_code", "label")

  val aDF = assocs
    .join(eDF, assocs("disease_id") === eDF("id"), "inner")
    .select(aCols.map(col):_*)
    .repartitionByRange(col("target_id"), col("therapeutic_label"))
    .drop("disease_id", "id")
    .persist(StorageLevel.DISK_ONLY)

  aDF
    .groupBy(col("target_id"), col("therapeutic_label"), col("path_code"))
    .agg(collect_set(col("label")).as("diseases"),
      approx_count_distinct(col("label")).as("diseases_count"))
    .where(col("diseases_count") > 1)
}

@main
def main(output: String = "assocs_by_targets/"): Unit = {
  println(s"running to $output")

  val sparkConf = new SparkConf()
    .setAppName("similarities-diseases")
    .setMaster("local[*]")

  implicit val ss = SparkSession.builder
    .config(sparkConf)
    .getOrCreate

  val ddf = buildGroupByTarget()
  ddf.write.json(output)
}
