package io.opentargets.platform.ddr

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object Associations extends LazyLogging {
  def parseFile(filename: String, directAssocs: Boolean, scoreThreshold: Double, evsThreshold: Long)(implicit ss: SparkSession): DataFrame = {
    val ff = ss.read
      .json(filename)

    val filteredFF = ff.filter((column("is_direct") === directAssocs) and
      (column("harmonic-sum.overall") geq scoreThreshold) and
      (column("evidence_count.total") geq evsThreshold))
      .select(column("target.id").as("target_id"), column("target.gene_info.symbol").as("target_symbol"),
        column("disease.id").as("disease_id"), column("disease.efo_info.label").as("disease_label"),
        column("harmonic-sum.overall").as("score"),
        column("evidence_count.total").as("count"))
      .persist

    logger.debug(s"filtered associations count is ${filteredFF.count()}")

    filteredFF
  }

  private[ddr] def schemaComposer(l: List[String], lType: DataType): StructType =
    StructType(l.map(x => StructField(x, lType)))
}
