import $ivy.`com.github.pathikrit::better-files:3.8.0`
import $ivy.`com.typesafe:config:1.3.4`
import $ivy.`com.github.fommil.netlib:all:1.1.2`
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
import org.apache.spark.ml.linalg.{DenseVector, Matrix, Vector, Vectors}
import org.apache.spark.ml.feature.VectorAssembler
import breeze.linalg._

object Loaders {
  def loadDrugList(path: String)(implicit ss: SparkSession): DataFrame = {
    val drugList = ss.read.json(path)
      .selectExpr("id as chembl_id", "synonyms", "pref_name", "trade_names")
      .withColumn("drug_names", array_distinct(array_union(col("trade_names"),array_union(array(col("pref_name")), col("synonyms")))))
      .withColumn("_drug_name", explode(col("drug_names")))
      .withColumn("drug_name", lower(col("_drug_name")))
      .select("chembl_id", "drug_name")
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
def main(drugSetPath: String, inputPathPrefix: String, outputPathPrefix: String): Unit = {
  val sparkConf = new SparkConf()
    .set("spark.driver.maxResultSize", "0")
    .setAppName("similarities-loaders")
    .setMaster("local[*]")

  implicit val ss = SparkSession.builder
    .config(sparkConf)
    .getOrCreate

  import ss.implicits._

  // the curated drug list we want
  val drugList = Loaders.loadDrugList(drugSetPath).cache()

  // load FDA raw lines
  val lines = Loaders.loadFDA(inputPathPrefix)

  val fdas = lines.withColumn("reaction", explode(col("patient.reaction")))
    // after explode this we will have reaction-drug pairs
    .withColumn("drug", explode(col("patient.drug")))
    // just the fields we want as columns
    .selectExpr("safetyreportid", "serious", "receivedate", "primarysourcecountry",
      "qualification",
      "lower(reaction.reactionmeddrapt) as reaction_reactionmeddrapt" ,
      "ifnull(lower(drug.medicinalproduct), '') as drug_medicinalproduct",
      "ifnull(drug.openfda.generic_name, array()) as drug_generic_name_list",
      "ifnull(drug.openfda.brand_name, array()) as drug_brand_name_list",
      "ifnull(drug.openfda.substance_name, array()) as drug_substance_name_list",
      "drug.drugcharacterization as drugcharacterization")
    // we dont need these columns anymore
    .drop("patient", "reaction", "drug", "_reaction")
    // delicated filter which should be looked at FDA API to double check
    .where(col("qualification").isInCollection(Seq("1", "2", "3")) and col("drugcharacterization") === "1")
    // drug names comes in a large collection of multiple synonyms but it comes spread across multiple fields
    .withColumn("drug_names", array_distinct(array_union(col("drug_brand_name_list"), array_union(array(col("drug_medicinalproduct")),
      array_union(col("drug_generic_name_list"), col("drug_substance_name_list"))))))
    // the final real drug name
    .withColumn("_drug_name", explode(col("drug_names")))
    .withColumn("drug_name", lower(col("_drug_name")))
    // rubbish out
    .drop("drug_generic_name_list", "drug_substance_name_list", "_drug_name")
    .where($"drug_name".isNotNull and $"reaction_reactionmeddrapt".isNotNull and
      $"safetyreportid".isNotNull)
    // and we will need this processed data later on
    .join(drugList, Seq("drug_name"), "inner")
    .persist(StorageLevel.DISK_ONLY)

  // total unique report ids count grouped by reaction
  val aggByReactions = fdas.groupBy(col("reaction_reactionmeddrapt"))
    .agg(countDistinct(col("safetyreportid")).as("uniq_report_ids_by_reaction"))

  // total unique report ids count grouped by drug name
  val aggByDrugs = fdas.groupBy(col("chembl_id"))
    .agg(countDistinct(col("safetyreportid")).as("uniq_report_ids_by_drug"))

  // total unique report ids
  val uniqReports = fdas.select("safetyreportid").distinct.count

  // per drug-reaction pair
  val doubleAgg = fdas.groupBy(col("chembl_id"), col("reaction_reactionmeddrapt"))
    .agg(countDistinct(col("safetyreportid")).as("uniq_report_ids"))
    .withColumnRenamed("uniq_report_ids", "A")
    .join(aggByDrugs, Seq("chembl_id"), "inner")
    .join(aggByReactions, Seq("reaction_reactionmeddrapt"), "inner")
    .withColumn("C", col("uniq_report_ids_by_drug") - col("A"))
    .withColumn("B", col("uniq_report_ids_by_reaction") - col("A"))
    .withColumn("D", lit(uniqReports) - col("uniq_report_ids_by_drug") - col("uniq_report_ids_by_reaction") + col("A"))
    .withColumn("aterm", $"A" * (log($"A") - log($"A" + $"B")))
    .withColumn("cterm", $"C" * (log($"C") - log($"C" + $"D")))
    .withColumn("acterm", ($"A" + $"C") * (log($"A" + $"C") - log($"A" + $"B" + $"C" + $"D")) )
    .withColumn("llr", when($"C" === 0, lit(0.0)).otherwise($"aterm" + $"cterm" - $"acterm"))
    // Max_iae (llr_ij) (all ae for a drug)
    // filter the drugs we want

  val critVal = doubleAgg.groupBy($"chembl_id")
    .agg(first($"C").as("C"),
      collect_list($"B").as("B"),
      first($"A").as("A"))

  val assembler = new VectorAssembler()
    .setInputCols(Array("B"))
    .setOutputCol("bV")

  val udfNorm = udf((v: DenseVector) => {
    val maxI = v.argmax
    v  v(maxI)
  })
  val critValV = assembler.transform(critVal)

  // https://gist.github.com/d0choa/9e4e197eae0310b4d045eb8aa5f13ec4#file-simple_llr_montecarlo-r-L18
  //logLRnum<-function(x,y, z, n){
  //  logLR<-x*(log(x)-log(y))+ (z-x)*(log(z-x)-log(n-y))
  //  return(logLR)
  //}
  //
  //## n_j is the total number of unique reports for the drug (int)
  //## n_i is the number of reports for event (vector)
  //## n is total number of unique reports overall (int)
  //## prob is probability of every event happening (ni / n) (vector)
  //getCritVal2 <- function(R, n_j, n_i, n, Pvector, prob){
  //#  set.seed(12) Simulatej
  //    I <- length(Pvector)
  //    ## Generate multinomially distributed random number vectors and compute multinomial probabilities
  //    ## https://stat.ethz.ch/R-manual/R-devel/library/stats/html/Multinom.html
  //    Simulatej<-rmultinom(R,size=n_j,prob=Pvector)
  //    myLLRs<-matrix(0,I,R)
  //    for (i in seq_along(Pvector)){
  //        for (j in 1:R){
  //            myLLRs[i,j]=logLRnum( Simulatej[i,j] ,n_i[i] , n_j, n )
  //        }
  //    }
  //    myLLRs <- myLLRs - n_j*log(n_j)+n_j*log(n)
  //    myLLRs[is.nan(myLLRs)] <- 0
  //    myLLRs[is.na(myLLRs)] <- 0
  //    mymax <- apply(myLLRs, 2, max)
  //    critval <- quantile(mymax,  probs = prob)
  //    critval01 <- quantile(mymax,  probs = .99)       #get cut off value
  //    return( list(critval=critval, critval01=critval01, mymax=mymax) )
  //}
  //
  //## the critical value is calculated for every drug independently
  //## R = 1000 permutations
  //mycritval <- getCritVal2(1000, out$totalbydrug[1], out$totalbyevent, totalreports, (out$totalbyevent / totalreports), .95)



  fdas.write.json(outputPathPrefix + "/fdas/")
  doubleAgg.write.json(outputPathPrefix + "/agg/")
  doubleAgg
    .join(fdas, Seq("chembl_id", "reaction_reactionmeddrapt"), "left")
    .write.json(outputPathPrefix + "/agg_with_fdas/")
}
