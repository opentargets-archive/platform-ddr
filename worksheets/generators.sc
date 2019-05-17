import $ivy.`org.apache.spark::spark-core:2.4.1`
import $ivy.`org.apache.spark::spark-sql:2.4.1`
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import $file.loaders
import org.apache.spark.SparkConf

object Generators {
  def similarTargetsByRNAExpression(expPath: String)(implicit ss: SparkSession): DataFrame = {
    val tissues = loaders.Loaders.loadExpression(expPath)

    tissues.show()
    val dfRNA = tissues
      .withColumn("organ_name", explode(col("organs")))
      .groupBy("organ_name", "label", "rna.level")
      .agg(collect_set(when(col("rna.level") >= 2, col("id"))).as("rna_genes"),
        collect_set(when(col("rna.zscore") >= 3, col("id"))).as("zscore_genes"),
        approx_count_distinct(when(col("rna.level") >= 2, col("id"))).as("rna_genes_counts"),
        approx_count_distinct(when(col("rna.zscore") >= 3, col("id"))).as("zscore_genes_counts"),
        approx_count_distinct(col("id")).as("genes_counts")
    ).persist

    val dfProtein = tissues
      .withColumn("organ_name", explode(col("organs")))
      .groupBy("organ_name", "label", "protein.level")
      .agg(collect_set(when(col("protein.level") > 0, col("id"))).as("protein_genes"),
          approx_count_distinct(when(col("protein.level") > 0, col("id"))).as("protein_genes_counts"),
          approx_count_distinct(col("id")).as("genes_counts")
    ).persist

    dfRNA.agg(max(col("zscore_genes_counts")).as("max_zscore"),
      avg(col("zscore_genes_counts")).as("avg_zscore"),
      stddev(col("zscore_genes_counts")).as("std_zscore"),
      max(col("rna_genes_counts")).as("max_rna"),
      avg(col("rna_genes_counts")).as("avg_rna"),
      stddev(col("rna_genes_counts")).as("std_rna"))
        .show(100, false)

    dfProtein.agg(max(col("protein_genes_counts")).as("max_protein"),
      avg(col("protein_genes_counts")).as("avg_protein"),
      stddev(col("protein_genes_counts")).as("std_protein"))
      .show(100, false)

    dfRNA.show()
    dfProtein.show()

    dfRNA
  }
}

@main
def main(output: String = "targets_similar_expression/"): Unit = {
  println(s"running to $output")

  val sparkConf = new SparkConf()
    .setAppName("similarities-targets-expression")
    .setMaster("local[*]")

  implicit val ss = SparkSession.builder
    .config(sparkConf)
    .getOrCreate

  val ddf = Generators.similarTargetsByRNAExpression("../19.04_expression-data.json")
  // ddf.write.json(output)
}
