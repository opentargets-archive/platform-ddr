import $ivy.`com.dongxiguo::fastring:1.0.0`
import $ivy.`com.github.pathikrit::better-files:3.8.0`
import $ivy.`com.typesafe.play::play-json:2.7.3`
import $ivy.`com.typesafe:config:1.3.4`
import $ivy.`org.apache.spark::spark-core:2.4.3`
import $ivy.`org.apache.spark::spark-mllib:2.4.3`
import $ivy.`org.apache.spark::spark-sql:2.4.3`
import better.files.Dsl._
import better.files._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import play.api.libs.json._

object Loaders {
  /** Load efo data from efo index dump so this allows us
    * to navegate through the ontology
    */
  def loadEFO(path: String)(implicit ss: SparkSession): DataFrame = {
    val genAncestors = udf((codes: Seq[Seq[String]]) =>
      codes.view.flatten.toSet.toSeq)

    val stripEfoID = udf((code: String) => code.split("/").last)
    val efos = ss.read.json(path)
      .withColumn("disease_id", stripEfoID(col("code")))
      .withColumn("path_code", genAncestors(col("path_codes")))
      .drop("paths")

    efos
      .repartitionByRange(col("disease_id"))
      .sortWithinPartitions(col("disease_id"))
  }

  /** Load gene data from gene index dump in order to have a comprehensive list
    * of genes with their symbol biotype and name
    */
  def loadGenes(path: String)(implicit ss: SparkSession): DataFrame = {
    val genes = ss.read.json(path)

    genes
      .withColumnRenamed("id", "target_id")
      .repartitionByRange(col("target_id"))
      .sortWithinPartitions(col("target_id"))
      .selectExpr("*", "_private.facets.*", "tractability.*")
      .drop("drugbank", "uniprot", "pfam", "reactome", "_private", "ortholog", "tractability")
  }

  /** Load expression data index dump and exploding the tissues vector so
    * having a tissue per row per gene id and mapping each tissue
    * estructure to multi column
    */
  def loadExpression(path: String)(implicit ss: SparkSession): DataFrame = {
    // val tissueCols = Seq("id", "_tissue.*")
    val tissues = ss.read.json(path)
      // .withColumn("_tissue", explode(col("tissues")))
      .withColumnRenamed("gene", "target_id")

    tissues
      .repartitionByRange(col("target_id"))
      .sortWithinPartitions(col("target_id"))
  }

  /** Load associations from ES index dump and filter by
    * - is_direct == True
    * - and then generate some columns as score target id and name and for diseases
    * get disease id and disease name
    * - cache list unique diseases and list unique targets
    */
  def loadAssociations(path: String)(implicit ss: SparkSession): DataFrame = {
    val assocs = ss.read.json(path)
      .withColumn("score", col("harmonic-sum.overall"))
      .withColumn("target_id", col("target.id"))
      .withColumn("disease_id", col("disease.id"))
      .withColumn("target_name", col("target.gene_info.symbol"))
      .withColumn("disease_name", col("disease.efo_info.label"))
      .withColumn("score_datasource", col("harmonic-sum.datasources"))
      .withColumn("score_datatype", col("harmonic-sum.datatypes"))
      .drop("private", "_private", "target", "disease", "id")
    assocs
  }

  def loadEvidences(path: String)(implicit ss: SparkSession): DataFrame = {
    val evidences = ss.read.json(path)
    evidences
      .withColumnRenamed("sourceID", "datasource")
      .withColumnRenamed("type", "datatype")
      .withColumn("target_id", col("target.id"))
      .withColumn("disease_id", col("disease.id"))
      .selectExpr("datasource", "datatype", "scores", "evidence", "target_id", "disease_id")
      // scores$association_score
      // datasource
      // datatype
  }
}

object DFImplicits {
  implicit class ImplicitDataFrameFunctions(df: DataFrame) {
    val fieldSeparator = "$"
    val charReplacement = "_"
    def flattenDataframe(rep: String = fieldSeparator): DataFrame = {
      def _mkColsFromStrings(names: Seq[String]): Seq[Column] = {
        val structFieldsNewNames = names.map(_.replace(".", fieldSeparator))
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

    def fixColumnNames(rep: String = charReplacement): DataFrame = {
      val fNames = df.schema.fields.map(_.name)
      fNames.foldLeft(df)((d, name) => d.withColumnRenamed(name, name
        .replace(" ", rep)
        .replace("-", rep)))
    }
  }
}

object Functions {
  def saveSchemaTo(df: DataFrame, filename: File): Unit =
    filename < df.schema.json

  def loadSchemaFrom(filename: String): StructType = {
    val lines = filename.toFile.contentAsString
    DataType.fromJson(lines).asInstanceOf[StructType]
  }
}

object SchemaConverter {
  private def seqFieldsFromJSON(json: JsValue): Seq[String] = {
    def fieldToString(obj: JsValue): String = {
      val name = (obj \ "name").as[String]
      name.replace("$", "__") + " Nullable(String)"
    }
    val fields = (json \ "fields").get
    fields match {
      case JsArray(value) => value.map(fieldToString)
      case _ => Seq.empty
    }
//    json match {
//    case JsObject(fields) => fields.keys ++ fields.values.flatMap(allKeys)
//    case JsArray(as) => as.flatMap(allKeys)
//    case _ => Seq.empty[String]
  }

  def fromString(schema: Option[String]): String => Option[String] = {
    val jo = Option(Json.parse(schema.getOrElse("")))
    apply(jo)
  }

  def apply(schema: Option[JsValue])(tableName: String): Option[String] = {
    schema.map(jo => {
      val tableTemplate =
        """
          |create table if not exists %s
          |%s
          |engine = Log;
        """.stripMargin

        tableTemplate.format(tableName, seqFieldsFromJSON(jo).mkString("(\n", ",\n", ")"))
    })
  }
}

@main
def main(schemaFilename: String): Unit = {
  val text = Option(schemaFilename.toFile.contentAsString)
  println(SchemaConverter.fromString(text)("table1"))
}