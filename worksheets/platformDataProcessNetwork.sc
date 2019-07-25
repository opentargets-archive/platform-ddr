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
import platformData.DFImplicits._
import platformData.{Functions, Loaders}

/** compute the network otar project and map to ensembl id and symbol per protein pair A <-> B
  * @param inputPathPrefix
  * @param outputPathPrefix
  */
@main
def main(inputPathPrefix: String, outputPathPrefix: String): Unit = {
  val sparkConf = new SparkConf()
    .set("spark.driver.maxResultSize", "0")
    .setAppName("similarities-loaders")
    .setMaster("local[*]")

  implicit val ss = SparkSession.builder
    .config(sparkConf)
    .getOrCreate

  // loading genes, flatten and fix column names
  val networkDB = Loaders.loadNetworkDB(inputPathPrefix + "protein_pair_interactions.json",
    inputPathPrefix + "19.04_gene-data.json")

  val netLUT = Loaders.loadNetworkDBLUT(inputPathPrefix + "protein_pair_interactions.json",
    inputPathPrefix + "19.04_gene-data.json", 0.45)

  networkDB.write.json(outputPathPrefix + "networkDB_unfiltered")
  netLUT.write.json(outputPathPrefix + "networkDBLUT_045")
}
