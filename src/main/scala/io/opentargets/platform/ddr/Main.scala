package io.opentargets.platform.ddr

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scopt.OptionParser


case class CommandLineArgs(inputFile: Option[String] = None,
                           outputPath: Option[String] = Some("output/"),
                           kwargs: Map[String, String] = Map())

object Main extends LazyLogging {
  val progName: String = "io-opentargets-platform-ddr"
  val entryText: String =
    """
      |
      |NOTE:
      |copy logback.xml locally, modify it with desired logger levels and specify
      |-Dlogback.configurationFile=/path/to/customised/logback.xml. Keep in mind
      |that "Logback-classic can scan for changes in its configuration file and
      |automatically reconfigure itself when the configuration file changes".
      |So, you even don't need to relaunch your process to change logging levels
      | -- https://goo.gl/HMXCqY
      |
    """.stripMargin

  def run(config: CommandLineArgs): Unit = {
    println(s"running $progName")

    logger.debug(s"running with cli args $config")
    val logLevel = config.kwargs.getOrElse("log-level", "ERROR")
    val sparkURI = config.kwargs.getOrElse("spark-uri", "local[*]")

    config.inputFile match {
      case Some(fname) =>
        // do code here
        val conf: SparkConf = new SparkConf()
          .setAppName(progName)
          .setMaster(sparkURI)

        implicit val ss: SparkSession = SparkSession.builder
          .config(conf)
          .getOrCreate

        logger.debug("setting sparkcontext logging level to log-level")
        ss.sparkContext.setLogLevel(logLevel)

        logger.info(s"process file $fname")

        val rawDF = ss.read.json(fname).persist()

        Relations(rawDF, 20).foreach(similaritiesDF => {
          logger.info(s"save dataframe to ${config.outputPath.get}")
          similaritiesDF.write.json(config.outputPath.get)
        })

        ss.stop

        println("closing app... done.")
      case None =>
        logger.error("failed to specify a filename; try --help")
    }
  }

  def main(args: Array[String]): Unit = {
    parser.parse(args, CommandLineArgs()) match {
      case Some(config) =>
        run(config)
      case None => println("problem parsing commandline args")
    }
  }

  val parser: OptionParser[CommandLineArgs] = new OptionParser[CommandLineArgs](progName) {
    head(progName)

    opt[String]("input-file")
      .abbr("i")
      .valueName("<filename>")
      .action((x, c) => c.copy(inputFile = Option(x)))
      .text("file contains all associations from the OT association dump (format: jsonl)")

    opt[String]("output-path")
      .abbr("o")
      .valueName("<path>")
      .action((x, c) => c.copy(outputPath = Option(x)))
      .text("output path where data-driven relations will be dump (default: output/)")

    opt[Map[String, String]]("kwargs")
      .valueName("k1=v1,k2=v2...")
      .action((x, c) => c.copy(kwargs = x))
      .text("other arguments")

    note(entryText)

    override def showUsageOnError = true
  }
}
