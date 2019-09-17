import $ivy.`org.apache.spark::spark-core:2.4.3`
import $ivy.`org.apache.spark::spark-sql:2.4.3`
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

/**
  * install sdkman and scala 2.11.12
  * install ammonite repl
  * export some jvm mem things like this export JAVA_OPTS="-Xms1G -Xmx80G"
  * execute it as amm script.sc
  * paths are hardcoded!
  */
object Loaders {
  def loadEvidences(path: String)(implicit ss: SparkSession): DataFrame = {
    val evidences = ss.read.json(path)
    evidences
  }
}

@main
def main(): Unit = {
  val sparkConf = new SparkConf()
    .setAppName("drugs-aggregation")
    .setMaster("local[*]")

  implicit val ss = SparkSession.builder
    .config(sparkConf)
    .getOrCreate

  val ddf = Loaders.loadEvidences("../19.06_evidence-data.json")

  val agg = ddf
      .where(col("private.datatype") === "known_drug")
    .groupBy(col("disease.id").as("disease_id"),
      col("drug.id").as("drug_id"),
      col("evidence.drug2clinic.clinical_trial_phase.label").as("clinical_trial_phase"),
      col("evidence.drug2clinic.status").as("clinical_trial_status"),
      col("target.target_name").as("target_name"))
    .agg(collect_list(col("evidence.drug2clinic.urls")).as("_list_urls"),
      count(col("evidence.drug2clinic.urls")).as("list_urls_counts"),
      first(col("drug.molecule_type")).as("drug_type"),
      first(col("evidence.target2drug.mechanism_of_action")).as("mechanism_of_action"),
      first(col("target.activity")).as("activity"),
      first(col("target.target_class")).as("target_class")
    )
      .withColumn("list_urls", flatten(col("_list_urls")))
      .drop("_list_urls")

  agg.write
    .json("evidences_agg_drugs/")
}
