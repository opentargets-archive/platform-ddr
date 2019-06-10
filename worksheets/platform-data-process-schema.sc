import $ivy.`org.apache.spark::spark-core:2.4.3`
import $ivy.`org.apache.spark::spark-sql:2.4.3`
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType

object Loaders {
  def generateOTDataSchema(path: String)(implicit ss: SparkSession): StructType = {
    val jsonSchema = ss.read.json(path)
      .schema

    jsonSchema
  }
}

@main
def main(inputPathPrefix: String): Unit = {
  val sparkConf = new SparkConf()
    .setAppName("similarities-loaders")
    .setMaster("local[*]")

  implicit val ss = SparkSession.builder
    .config(sparkConf)
    .getOrCreate

  val schema = Loaders.generateOTDataSchema(inputPathPrefix)
  println(schema.json)

}
