import $ivy.`org.apache.spark::spark-core:2.4.3`
import $ivy.`org.apache.spark::spark-sql:2.4.3`
import org.apache.spark.sql.functions._

object Functions {

  def addValue =
    udf((el: String, array: Seq[String]) => array ++ Array(el))

  def addList =
    udf((el: Seq[String], array: Seq[Seq[String]]) => array ++ Array(el))

  def getDuplicates =
    udf((xs: Seq[String]) => {
      xs.view.groupBy(identity)
        .collect { case (x,ys) if ys.lengthCompare(1) > 0 => x }.toSeq
    })
}