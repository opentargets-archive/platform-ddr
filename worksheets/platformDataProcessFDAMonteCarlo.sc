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
import org.apache.spark.ml.linalg.{DenseVector, Matrix, Vectors}
import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, Vector => BV, DenseMatrix => BDM}

object Loaders {
  def loadAggFDA(path: String)(implicit ss: SparkSession): DataFrame =
    ss.read.json(path)
}

@main
def main(inputPath: String, outputPathPrefix: String): Unit = {
  val sparkConf = new SparkConf()
    .set("spark.driver.maxResultSize", "0")
    .setAppName("similarities-loaders")
    .setMaster("local[*]")

  implicit val ss = SparkSession.builder
    .config(sparkConf)
    .getOrCreate

  import ss.implicits._

  val fdas = Loaders.loadAggFDA(inputPath)

  val udfProbVector = udf((permutations: Int, n_j: Int, n_i: Seq[Long], n: Int, prob: Double) => {
    import breeze.linalg._
    import breeze.stats._
    // get the Pvector normalised by max element not sure i need to do it
    val nj = n_j.toDouble
    val N = n.toDouble
    val ni = convert(BDV(n_i.toArray), Double)
    val Pvector = ni /:/ N
    Pvector := Pvector /:/ breeze.linalg.max(Pvector)

    // generate the multinorm permutations
    val mult = breeze.stats.distributions.Multinomial(Pvector)
    val mp = BDM.zeros[Double](Pvector.size, permutations)
    val llrs = BDV.zeros[Double](Pvector.size)

    // generate n_i columns
    for (i <- 0 until ni.size) {
      mp(i,::) := ni(mult.samples.take(permutations).toSeq).t
    }

    // compute all llrs in one go
    val logmp: BDM[Double] = breeze.numerics.log(mp)
    val zx: BDM[Double] = mp - nj
    val logzx: BDM[Double] = breeze.numerics.log(zx)
    val logNni: BDV[Double] = breeze.numerics.log(N - ni)

    // logLR <- x * (log(x) - log(y)) + (z-x) * (log(z - x) - log(n - y))
    // myLLRs <- myLLRs - n_j * log(n_j) + n_j * log(n)

    mp := mp *:* (logmp(::,*) - breeze.numerics.log(ni)) + zx *:* (logzx(::,*) - logNni)
    mp := mp - nj * math.log(nj) + nj * math.log(N)

    mp(mp.findAll(v => v.isNaN)) := 0.0
    llrs := breeze.linalg.max(mp(*,::))

    // get the prob percentile value as a critical value
    DescriptiveStats.percentile(llrs.data, prob)
  })

  val critVal = fdas
    .where($"chembl_id" === "CHEMBL25")
    .withColumn("n_j", $"C" + $"A")
    .withColumn("n_i", $"B" + $"A")
    .withColumn("n", $"D" + $"n_j" + $"n_i" - $"A")
    .groupBy($"chembl_id")
    .agg(first($"n_j").as("n_j"),
      collect_list($"n_i").as("n_i"),
      first($"n").as("n"))
    .withColumn("critVal", udfProbVector(lit(1000), $"n_j", $"n_i", $"n", lit(0.95)))

//  fdas.join(critVal.select("chembl_id", "critVal"), Seq("chembl_id"), "inner")
  fdas.join(critVal, Seq("chembl_id"), "inner")
    .write
    .json(outputPathPrefix + "/agg_critval/")

  // https://gist.github.com/d0choa/9e4e197eae0310b4d045eb8aa5f13ec4#file-simple_llr_montecarlo-r-L18
  //## n_j is the total number of unique reports for the drug (int) uniq_report_ids_by_drug
  //## n_i is the number of reports for event (vector) uniq_report_ids_by_reaction
  //## n is total number of unique reports overall (int) uniqReports
  //## Pvector (n_i / n) (all events for a single drug)
  //## prob is probability of every event happening Pvector (vector) = 0.95
  //    ##  set.seed(12)
  //    I <- length(Pvector)
  //    Simulatej<-rmultinom(R,size=n_j,prob=Pvector)
  //    myLLRs <- t(sapply(1:length(Pvector), function(i){ # each event across all permutations
  //        logLRnum(Simulatej[i, ], n_i[i], n_j, n) (passing a vector simulatej[i,]
  //    }))
  //    myLLRs <- myLLRs - n_j * log(n_j) + n_j * log(n)
  //    myLLRs[is.na(myLLRs)] <- 0
  //    mymax <- apply(myLLRs, 2, max) (by column (permutation-wise get the max LLR)
  //    critval <- quantile(mymax,  probs = prob) (get the top quantile per final vector of max llrs permutation across
  //    return(critval)
  //}
  //logLRnum<-function(x, y, z, n){
  //  logLR <- x * (log(x) - log(y)) + (z-x) * (log(z - x) - log(n - y))
  //  return(logLR)
  //}
  //permutations <- 1000
  //prob <- 0.95
  //all <- all %>%
  //    group_by(drug) %>%
  //    mutate(critval = getCritVal(permutations,
  //                                totalbydrug[1],
  //                                totalbyreaction,
  //                                totalreports[1],
  //                                pvector,
  //                                prob)) %>%
  //    mutate(significant = llr > critval)


}
