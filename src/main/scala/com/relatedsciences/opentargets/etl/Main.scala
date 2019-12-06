/**
  * Scoring pipeline execution for spark-submit
  * Usage:
  * /usr/spark-2.4.4/bin/spark-submit \
  * --driver-memory 12g \
  * --class "com.relatedsciences.opentargets.pipeline.Main" \
  * target/scala-2.12/ot-scoring_2.12-0.1.jar
  */
package com.relatedsciences.opentargets.etl
import java.nio.file.Paths

import com.relatedsciences.opentargets.etl.configuration.Configuration.Config
import org.apache.spark.sql.SparkSession
import com.typesafe.scalalogging.LazyLogging
import pureconfig.error.ConfigReaderFailures
import scopt.OptionParser
import pureconfig.generic.auto._
import pureconfig._

object Main extends LazyLogging {
  val progName: String = "ot-platform-pipe"
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

  case class Args(config: String = "", command: Option[String] = None)

  def run(args: Args, config: Config)(implicit ss: SparkSession): Unit = {
    println(s"running $progName")

    logger.debug(s"running with cli args $args and with configuration $config")

    // TODO: check command set
    val cmd = Command.CommandEnum.withName(args.command.get)
    logger.info(s"Running command $cmd")
    cmd.enumEntry.factory(ss, config).run()

    println("closing app... done.")
  }

  private def getSparkSession(config: Config) = {
    SparkSession.builder
      .appName(progName)
      .config("spark.master", if (config.sparkUri.nonEmpty) config.sparkUri else "local[*]")
      .getOrCreate()
  }

  def loadConfig(file: String): Either[ConfigReaderFailures, Config] = { //implicit reader used to read the config file
    val conf = if (file.nonEmpty) {
      logger.info(s"loading configuration from commandline as $file")
      ConfigSource.file(Paths.get(file)).load[Config]
    } else {
      logger.info("load configuration from package resource")
      ConfigSource.default.load[Config]
    }
    conf
  }

  def main(args: Array[String]): Unit = {
    // parser.parse returns Option[C]
    parser.parse(args, Args()) match {
      case Some(args) =>
        loadConfig(args.config) match {
          case Right(config) => {
            implicit val ss: SparkSession = getSparkSession(config)
            ss.sparkContext.setLogLevel(config.logLevel)
            try {
              run(args, config)
            } finally {
              ss.close()
            }
          }
          case Left(failures) =>
            println(s"configuration contains errors like ${failures.toString}")
        }

      case None => println("problem parsing commandline args")
    }
  }

  val parser: OptionParser[Args] =
    new OptionParser[Args](progName) {
      head(progName)

      opt[String]("config")
        .abbr("c")
        .valueName("<config-file>")
        .action((x, c) => c.copy(config = x))
        .text("file contains the configuration needed to run the pipeline")

      Command.CommandEnum.values.foreach(
        command =>
          cmd(command.entryName)
            .action((_, c) => c.copy(command = Some(command.entryName)))
            .text("some command")
      )
      opt[String]("config")
        .abbr("c")
        .valueName("<config-file>")
        .action((x, c) => c.copy(config = x))
        .text("file contains the configuration needed to run the pipeline")

//      opt[Map[String, String]]("kwargs")
//        .valueName("k1=v1,k2=v2...")
//        .action((x, c) => c.copy(kwargs = x))
//        .text("other arguments")
//
//      cmd("distance-nearest")
//        .action((_, c) => c.copy(command = Some("distance-nearest")))
//        .text("generate distance nearest based dataset")

      note(entryText)

      override def showUsageOnError = true
    }
}
