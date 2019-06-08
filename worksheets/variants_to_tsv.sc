import $ivy.`org.apache.spark::spark-core:2.4.3`
import $ivy.`org.apache.spark::spark-sql:2.4.3`
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel


@main
def main(input: String, output: String): Unit = {
  val refChromosomes = (1 to 22).map(_.toString) ++ Seq("X", "Y", "MT")
  val sparkConf = new SparkConf()
    .setAppName("similarities-loaders")
    .setMaster("local[*]")

  implicit val ss = SparkSession.builder
    .config(sparkConf)
    .getOrCreate

  val variants = ss.read.parquet(input)
    .selectExpr("*", "cadd.*", "af.*")
    .drop("cadd", "af")
    .filter(col("chr_id") isin(refChromosomes:_*))

  variants.repartition(col("chr_id"))
    .sort(col("chr_id"), col("position"))
    .coalesce(1)
    .write
    .option("sep", "\t")
    .option("header", "true")
    .option("compression", "gzip")
    .csv(output)
}
