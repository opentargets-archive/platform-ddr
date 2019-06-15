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
    def _mkColsFromStrings(names: Seq[String]): Seq[Column] = {
      val structFieldsNewNames = names.map(_.replace(".", "_"))
      (names zip structFieldsNewNames).map(e => col(e._1).as(e._2))
    }

    def _flattenDataFrame(df: DataFrame): DataFrame = {
      val fdf = allStructColumnNames(df.schema.fields)

      if (fdf.isEmpty) {
        df
      } else {
        val ddf = df.select(col("*") +: fdf.flatMap(e => flattenArray(e) ++ flattenStruct(e)): _*)
          .drop(fdf.map(_.name): _*)

        _flattenDataFrame(ddf)
      }
    }

    def flattenStruct(field: StructField): Seq[Column] = {
      field.dataType match {
        case sType: StructType =>
          val structFields = sType.fields.map(e => (field.name :: e.name :: Nil).mkString("."))
          _mkColsFromStrings(structFields)
        case _ => Seq.empty
      }
    }

    def flattenArray(field: StructField): Seq[Column] = {
      field.dataType match {
        case sType: ArrayType =>
          sType.elementType match {
            case asType: StructType =>
              val structFields = asType.fields.map(e => (field.name :: e.name :: Nil).mkString("."))
              _mkColsFromStrings(structFields)
          }
        case _ => Seq.empty
      }
    }

    def allStructColumnNames(xs: Seq[StructField]): Seq[StructField] =
      xs.filter(_.dataType match {
        case _: StructType => true
        case sArray: ArrayType =>
          sArray.elementType match {
            case _: StructType => true
            case _ => false
          }
        case _ => false
      })

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
    .write.json("esr1_flatten/")

//  val schema = Loaders.generateOTDataSchema(inputPathPrefix)
//  println(schema.json)
}
