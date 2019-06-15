import $ivy.`com.github.pathikrit::better-files:3.8.0`
import $ivy.`org.apache.spark::spark-core:2.4.3`
import $ivy.`org.apache.spark::spark-sql:2.4.3`

import better.files._
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.StructType

import scala.io._

object Loaders {
  def generateOTDataSchema(path: String)(implicit ss: SparkSession): StructType = {
    val jsonSchema = ss.read.json(path)
      .schema

    jsonSchema
  }
}

object Appliers {
  def flattenDataframe(df: DataFrame): DataFrame = {
    def flattenStruct(field: StructField): Seq[Column] = {
      field.dataType match {
        case sType: StructType =>
          val structFields = sType.fields.map(e => (field.name :: e.name :: Nil).mkString("."))
          val structFieldsNewNames = structFields.map(_.replace(".", "_"))
          (structFields zip structFieldsNewNames).map(e => col(e._1).as(e._2))
      }
    }

    def flattenArray(field: StructField): Seq[Column] = ???

    def allStructColumnNames(xs: Seq[StructField]): Seq[StructField] =
      xs.filter(_.dataType match {
        case _: StructType => true
        case _ => false
      })

    def _flattenDataFrame(df: DataFrame): DataFrame = {
      val fdf = allStructColumnNames(df.schema.fields)

      if (fdf.isEmpty) {
        df
      } else {
        val ddf = df.select(col("*") +: fdf.flatMap(flattenStruct): _*)
          .drop(fdf.map(_.name): _*)

        _flattenDataFrame(ddf)
      }
    }

    _flattenDataFrame(df)
  }
}

@main
def main(inputPathPrefix: String, jsonSchemaFilename: String): Unit = {
  val sparkConf = new SparkConf()
    .setAppName("similarities-loaders")
    .setMaster("local[*]")

  implicit val ss = SparkSession.builder
    .config(sparkConf)
    .getOrCreate

  val lines = jsonSchemaFilename.toFile.contentAsString
  val newSchema=DataType.fromJson(lines).asInstanceOf[StructType]
  val esr1 = ss.read.schema(newSchema).json(inputPathPrefix)

  Appliers.flattenDataframe(esr1)
    .selectExpr("*", "cancerbiomarkers.drug as cancerbiomarkers_drug")
    .write.json("esr1_flatten/")

//  val schema = Loaders.generateOTDataSchema(inputPathPrefix)
//  println(schema.json)
}
