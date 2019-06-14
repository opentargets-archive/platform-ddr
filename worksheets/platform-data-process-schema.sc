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
  ss.read.schema(newSchema).json(inputPathPrefix).where(col("target_id") === "ENSG00000091831")
    .write.json("esr1.json")

//  val schema = Loaders.generateOTDataSchema(inputPathPrefix)
//  println(schema.json)
}
