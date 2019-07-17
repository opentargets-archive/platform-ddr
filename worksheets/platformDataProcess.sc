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

@main
def main(inputPathPrefix: String, outputPathPrefix: String): Unit = {
  val sparkConf = new SparkConf()
    .set("spark.driver.maxResultSize", "0")
    .setAppName("similarities-loaders")
    .setMaster("local[*]")

  implicit val ss = SparkSession.builder
    .config(sparkConf)
    .getOrCreate

  val sdb = Loaders.loadStringDB(inputPathPrefix + "9606.protein.links.detailed.v11.0.txt",
    inputPathPrefix + "9606.protein.info.v11.0.txt")

  // loading genes, flatten and fix column names
  val geneDF = Loaders.loadGenes(inputPathPrefix + "19.04_gene-data.json")
  Functions.saveJSONSchemaTo(geneDF, outputPathPrefix.toFile, "target")

  val genes = geneDF
    .flattenDataframe()
    .fixColumnNames()

  val sym2id = genes.select("approved_symbol", "target__id").cache
  val sym2idMap = ss.sparkContext.broadcast(sym2id.collect.map(r => (r.getString(0), r.getString(1))).toMap)
  val mapList = udf((s: Seq[String]) => {
    s.map(sym2idMap.value.withDefaultValue("")).filter(_.nonEmpty).distinct
  })

  val geneSym2Id = sdb
    .join(sym2id, Seq("approved_symbol"), "inner")
    .drop("approved_symbol")
    .withColumnRenamed("nodes", "_nodes")
    .withColumn("nodes", mapList(col("_nodes")))

  val genesWithNodes = genes.join(geneSym2Id, Seq("target__id"), "left_outer")

  genesWithNodes.write.json(outputPathPrefix + "targets/")
  Functions.saveJSONSchemaTo(genesWithNodes, outputPathPrefix / "targets")
  Functions.saveSQLSchemaTo(genesWithNodes, outputPathPrefix / "targets", "ot.targets")

  // loading diseases, flatten and fix column names
  val diseaseDF = Loaders.loadEFO(inputPathPrefix + "19.04_efo-data.json")
  Functions.saveJSONSchemaTo(diseaseDF, outputPathPrefix.toFile, "disease")

  val diseases = diseaseDF
    .flattenDataframe()
    .fixColumnNames()

  diseases.write.json(outputPathPrefix + "diseases/")
  Functions.saveJSONSchemaTo(diseases, outputPathPrefix / "diseases")
  Functions.saveSQLSchemaTo(diseases, outputPathPrefix / "diseases", "ot.diseases")

  val expressionDF = Loaders.loadExpression(inputPathPrefix + "19.04_expression-data.json")
  Functions.saveJSONSchemaTo(expressionDF, outputPathPrefix.toFile, "expression")

  val expression = expressionDF
    .flattenDataframe()
    .fixColumnNames()
  expression.write.json(outputPathPrefix + "expression/")
  Functions.saveJSONSchemaTo(expression, outputPathPrefix / "expression")
  Functions.saveSQLSchemaTo(expression, outputPathPrefix / "expression", "ot.expression")

  val pureEvidences = Loaders.loadEvidences(inputPathPrefix + "19.04_evidence-data.json")
  Functions.saveJSONSchemaTo(pureEvidences, outputPathPrefix.toFile, "evidence")

  val flattenEvs = pureEvidences
    .flattenDataframe()
    .fixColumnNames()

  val pivotScores = pureEvidences
      .groupBy(col("evs_id"))
      .pivot(col("datasource"))
      .agg(first(col("scores.association_score")))
      .na.fill(0.0)

  val zNames = pivotScores.schema.fieldNames.withFilter(_ != "evs_id").map(e => (e, s"ds__$e"))
  val pScores = zNames.foldLeft(pivotScores)((df , zname) => df.withColumnRenamed(zname._1, zname._2))

  val joinEvs = flattenEvs.join(pScores, Seq("evs_id"))
      .join(diseases, Seq("disease__id"), "inner")

  joinEvs.write.json(outputPathPrefix + "evidences")
  Functions.saveJSONSchemaTo(joinEvs, outputPathPrefix / "evidences")
  Functions.saveSQLSchemaTo(joinEvs, outputPathPrefix / "evidences" , "ot.evidences")
}
