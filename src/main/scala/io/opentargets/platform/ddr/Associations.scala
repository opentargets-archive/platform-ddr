package io.opentargets.platform.ddr

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object Associations {
  val dataSources = List("uniprot", "slapenrich", "gene2phenotype", "uniprot_somatic", "uniprot_literature",
    "gwas_catalog", "reactome", "chembl", "intogen", "expression_atlas", "genomics_england", "phenodigm",
    "sysbio", "phewas_catalog", "eva_somatic", "progeny", "postgap", "eva", "cancer_gene_census", "europepmc")

  val dataTypes = List("rna_expression", "known_drug", "animal_model", "affected_pathway", "somatic_mutation",
    "genetic_association", "literature")

  val schemaEvidenceCount = StructType(StructField("total", LongType) ::
    StructField("datasources", schemaComposer(dataSources, LongType)) ::
    StructField("datatypes", schemaComposer(dataTypes, LongType)) :: Nil)

  val schemaAssociationScore = StructType(StructField("overall", DoubleType) ::
    StructField("datasources", schemaComposer(dataSources, DoubleType)) ::
    StructField("datatypes", schemaComposer(dataTypes, DoubleType)) :: Nil)

  val schemaTractability = StructType(
    StructField("smallmolecule", StructType(
      StructField("top_category", StringType) ::
        StructField("small_molecule_genome_member", BooleanType) ::
        StructField("buckets", ArrayType(LongType)) ::
        StructField("ensemble", DoubleType) ::
        StructField("high_quality_compounds", LongType) ::
        StructField("categories", StructType(
          StructField("clinical_precedence", LongType) ::
            StructField("predicted_tractable", LongType) ::
            StructField("discovery_precedence", LongType) :: Nil)) :: Nil)) ::
      StructField("antibody", StructType(
        StructField("buckets", ArrayType(LongType)) ::
          StructField("top_category", StringType) ::
          StructField("categories", StructType(
            StructField("predicted_tractable_med_low_confidence", DoubleType) ::
              StructField("predicted_tractable_high_confidence", DoubleType) ::
              StructField("clinical_precedence", DoubleType) :: Nil)) :: Nil)) :: Nil)

  val schemaGeneInfo = StructType(StructField("symbol", StringType) ::
    StructField("name", StringType) :: Nil)

  val schemaTarget = StructType(StructField("id", StringType, nullable = false) ::
    StructField("gene_info", schemaGeneInfo) ::
    StructField("tractability", schemaTractability) :: Nil)

  val schemaEfoInfo = StructType(StructField("label", StringType) ::
    StructField("therapeutic_area", StructType(StructField("codes", ArrayType(StringType)) ::
      StructField("labels", ArrayType(StringType)) :: Nil)) ::
    StructField("path", ArrayType(ArrayType(StringType))) :: Nil)

  val schemaDisease = StructType(StructField("id", StringType, nullable = false) ::
    StructField("efo_info", schemaEfoInfo) :: Nil)

  val schema = StructType(
    StructField("id", StringType, nullable = false) ::
      StructField("is_direct", BooleanType, nullable = false) ::
      StructField("evidence_count", schemaEvidenceCount) ::
      StructField("target", schemaTarget) ::
      StructField("disease", schemaDisease) ::
      StructField("association_score", schemaAssociationScore) :: Nil)

  def parseFile(filename: String, directAssocs: Boolean, scoreThreshold: Double)(implicit ss: SparkSession): DataFrame = {
    val ff = ss.read
      .option("badRecordsPath", "/tmp/badRecordsPath")
      //.schema(schema)
      .json(filename)

    ff.filter(column("is_direct") === directAssocs and column("association_score.overall") geq scoreThreshold)
      .persist
  }

  def computeSimilarTargets(df: DataFrame): DataFrame = ???

  def computeSimilarDiseases(df: DataFrame): DataFrame = ???

  private[ddr] def schemaComposer(l: List[String], lType: DataType): StructType =
    StructType(l.map(x => StructField(x, lType)))
}
