import $ivy.`com.github.pathikrit::better-files:3.8.0`
import $ivy.`com.typesafe:config:1.3.4`
import $ivy.`org.apache.spark::spark-core:2.4.3`
import $ivy.`org.apache.spark::spark-mllib:2.4.3`
import $ivy.`org.apache.spark::spark-sql:2.4.3`
import better.files.Dsl._
import better.files._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

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
      .drop("paths", "private", "_private")

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
      .drop("drugbank", "uniprot", "pfam", "reactome", "_private", "ortholog", "tractability",
        "mouse_phenotypes")
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
      .drop("_private", "private")
      .selectExpr("datasource", "datatype", "scores", "evidence", "target_id", "disease_id")
  }
}

object DFImplicits {
  implicit class ImplicitDataFrameFunctions(df: DataFrame) {
    val fieldSeparator = "__"
    val charReplacement = "_"

    def flattenDataframe(rep: String = fieldSeparator): DataFrame = {

      def mkColsFromStrings(names: Seq[String], nLevel: Int): Seq[Column] = {
        val structFieldsNewNames = names.map(_.replace(".", fieldSeparator))
        val cols = (names zip structFieldsNewNames).map(e => {
          if (e._1.contains(".")) {
            if (nLevel > 1)
              to_json(col(e._1)).as(e._2 + "_j")
            else
              col(e._1).as(e._2)
          } else {
            col(e._1)
          }
        })

        cols.foreach(println)
        cols
      }

      def _flattenDataFrame(df: DataFrame): DataFrame = {
        val fields = df.schema.fields.flatMap(e => e.dataType match {
          case st: StructType => flattenStruct(e.name :: Nil, st, 0)
          case at: ArrayType => flattenArray(e.name :: Nil, at, 1)
          case _ => mkColsFromStrings(Seq(e.name), 0)
        })

        df.select(fields: _*)
      }

      def flattenStruct(parent: Seq[String], struct: StructType, arrayLevel: Int): Seq[Column] = {
        if (arrayLevel > 1) {
          mkColsFromStrings(Seq(parent.filter(_.length > 0).mkString(".")), arrayLevel)
        } else {
          struct.fields.flatMap(e => {
            e.dataType match {
              case st: StructType => flattenStruct(parent :+ e.name, st, arrayLevel)
              case at: ArrayType => flattenArray(parent :+ e.name, at, arrayLevel + 1)
              case _ =>
                mkColsFromStrings(Seq((parent :+ e.name).filter(_.length > 0).mkString(".")), arrayLevel)
            }
          })
        }
      }

      def flattenArray(parent: Seq[String], fType: ArrayType, arrayLevel: Int): Seq[Column] = {
        if (arrayLevel > 1) {
          mkColsFromStrings(Seq(parent.filter(_.length > 0).mkString(".")), arrayLevel)
        } else {
          fType.elementType match {
            case sType: StructType => flattenStruct(parent, sType, arrayLevel)
            case aType: ArrayType => flattenArray(parent, aType, arrayLevel + 1)
            case _ =>
              mkColsFromStrings(Seq(parent.filter(_.length > 0).mkString(".")), arrayLevel)
          }
        }
      }
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
  def saveSchemaTo(df: DataFrame, jsonFile: File, sqlFile: File, tableName: String): Unit = {
    jsonFile < df.schema.json
    sqlFile < SchemaConverter(Some(df.schema))(tableName).get
  }

  def loadSchemaFrom(filename: String): Option[StructType] = {
    val lines = filename.toFile.contentAsString
    Option(DataType.fromJson(lines).asInstanceOf[StructType])
  }
}

object SchemaConverter {
  private def struct2SQL(struct: StructType): Seq[String] = {
    def fCast(sf: DataType): String = {
      sf match {
        case _: BooleanType => "UInt8"
        case _: IntegerType => "Int32"
        case _: LongType => "Int64"
        case _: FloatType => "Float32"
        case _: DoubleType => "Float64"
        case _: StringType => "String"
        case s: StructType => s.fields.map(f => s"Nullable(${fCast(f.dataType)})").mkString("Tuple(", ",", ")")
        case l: ArrayType => s"Nullable(${fCast(l.elementType)})".mkString("Array(", "", ")")
        case _ => "UnsupportedType"
      }
    }

    def metaCast(sf: StructField, data: String): String = {
      sf.dataType match {
        case a: ArrayType => a.elementType match {
          case _: ArrayType => s"$data default [[]]"
          case _ => s"$data default []"
        }

        case _ => if (sf.nullable) {
          s"Nullable($data)"
        } else {
          data
        }
      }
    }

    struct.fields.map(st => {
      "`" + st.name.replace("$", "__") + "` " + metaCast(st, fCast(st.dataType))
    })
  }

  def apply(schema: Option[StructType])(tableName: String): Option[String] = {
    schema.map(jo => {
      val tableTemplate =
        """
          |create table if not exists %s
          |%s
          |engine = Log;
        """.stripMargin

        tableTemplate.format(tableName, struct2SQL(jo).mkString("(\n", ",\n", ")"))
    })
  }
}

@main
def main(schemaFilename: String, tableName: String): Unit = {
  val schema = Functions.loadSchemaFrom(schemaFilename)
  println(SchemaConverter(schema)(tableName).get)
}