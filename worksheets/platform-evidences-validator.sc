import $ivy.`org.apache.spark::spark-core:2.4.3`
import $ivy.`org.apache.spark::spark-sql:2.4.3`
import $ivy.`sh.almond::ammonite-spark:0.4.2`
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._


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
      .withColumn("path_code", genAncestors(col("path_codes")))

    efos
      .repartitionByRange(col("id"))
      .sortWithinPartitions(col("id"))
  }

  /** Load gene data from gene index dump in order to have a comprehensive list
    * of genes with their symbol biotype and name
    */
  def loadGenes(path: String)(implicit ss: SparkSession): DataFrame = {
    val geneCols = Seq("id", "biotype", "approved_symbol", "approved_name", "go")
    val genes = ss.read.json(path)

    genes
      .drop("_private")
      .repartitionByRange(col("id"))
      .sortWithinPartitions(col("id"))
  }

  def loadEvidences(path: String)(implicit ss: SparkSession): DataFrame = {
    val struct2SortedValues = udf((r: Row) => {
      val fields = r.schema.fieldNames.sorted
      val values = r.getValuesMap[String](fields)
      values.map(_.toString).mkString
    })
    val evidences = ss
      .read
      .option("mode", "DROPMALFORMED")
      .option("badRecordsPath", "fails/")
      .json(path)

    evidences
      .withColumn("filename", input_file_name)
      .withColumn("hash_raw", struct2SortedValues(col("unique_association_fields")))
      .withColumn("hash_digest",sha2(col("hash_raw"), 256))
  }
}

@main
def main(evidencePath: String, outputPathPrefix: String = "out/"): Unit = {
  val sparkConf = new SparkConf().setAppName("similarities-loaders").setMaster("local[*]")

  implicit val ss = SparkSession.builder
    .config(sparkConf)
    .getOrCreate

//  val genes = Loaders.loadGenes(inputPathPrefix + "19.04_gene-data.json")
//  val diseases = Loaders.loadEFO(inputPathPrefix + "19.04_efo-data.json")

  val evidences = Loaders.loadEvidences(evidencePath)
  evidences
    // .selectExpr("element_at(split(filename,'/'),-1) as basename", "hash_digest", "hash_raw")
    .withColumn("basename", expr("element_at(split(filename,'/'),-1)"))
    .withColumn("basename2", split(col("filename"),"/").getItem(-1))
    .select("basename", "basename2", "hash_digest", "hash_raw")
    .show

  println(evidences.schema.sql)

  /**
    * compute count of same hashes and save out the ones > 1
    *
    * explanation_type "type of issue"
    * explanation_str "description of the issue"
    * use a when.otherwise to set all filters
    *
    * check type field move to data_type
    * check sourceID field move to data_source
    */
}
