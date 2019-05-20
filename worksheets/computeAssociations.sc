import $ivy.`org.apache.spark::spark-core:2.4.3`
import $ivy.`org.apache.spark::spark-sql:2.4.3`
import org.apache.spark._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import $file.loaders
import org.apache.spark.storage.StorageLevel

def buildAssociations()(implicit ss: SparkSession): DataFrame = {
//  val eDF = loaders.Loaders.loadEFO("../19.02_efo-data.json")
//    .repartitionByRange(col("id"))
//    .orderBy(col("id"))
//    .cache()
//
//  val assocs = loaders.Loaders.loadAssociations("../19.02_association-data.json")
//    .where(col("score") >= 0.1)
//    .repartitionByRange(col("disease_id"))
//    .orderBy(col("disease_id"))
//    .persist(StorageLevel.DISK_ONLY)
//
//  //  val aCols = Seq("target_id", "target_name", "disease_id", "disease_name", "score", "go_id", "go_term", "organ_name")
//  val aCols = Seq("target_id", "disease_id", "id", "therapeutic_label", "path_code", "label")
//
//  val aDF = assocs
//    .join(eDF, assocs("disease_id") === eDF("id"), "inner")
//    .select(aCols.map(col):_*)
//    .repartitionByRange(col("target_id"), col("therapeutic_label"))
//    .drop("disease_id", "id")
//    .persist(StorageLevel.DISK_ONLY)
//
//  aDF
//    .groupBy(col("target_id"), col("therapeutic_label"), col("path_code"))
//    .agg(collect_set(col("label")).as("diseases"),
//      approx_count_distinct(col("label")).as("diseases_count"))
//    .where(col("diseases_count") > 1)
  val evidences = loaders.Loaders.loadEvidences("../ENSG*")

  evidences
}

@main
def main(output: String = "assocs_by_targets/"): Unit = {
  println(s"running to $output")

  val sparkConf = new SparkConf()
    .setAppName("similarities-targets")
    .setMaster("local[*]")

  implicit val ss = SparkSession.builder
    .config(sparkConf)
    .getOrCreate

  val ddf = buildAssociations()
  // ddf.write.json(output)
}
