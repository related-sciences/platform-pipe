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
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession
import pureconfig._
import pureconfig.error.ConfigReaderFailures
import scopt.OptionParser

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

  private def getSparkSession(config: Config) = {
    SparkSession.builder
      .appName(progName)
      .config("spark.master", if (config.sparkUri.nonEmpty) config.sparkUri else "local[*]")
      .getOrCreate()
  }

  private def getConfig(file: String): Either[ConfigReaderFailures, Config] = { //implicit reader used to read the config file
    val conf = if (file.nonEmpty) {
      logger.info(s"loading configuration from commandline as $file")
      ConfigSource.file(Paths.get(file)).load[Config]
    } else {
      logger.info("load configuration from package resource")
      ConfigSource.default.load[Config]
    }
    conf
  }

  val parser: OptionParser[Args] = {
    new OptionParser[Args](progName) {
      head(progName)

      opt[String]("config")
        .abbr("c")
        .valueName("<config-file>")
        .action((x, c) => c.copy(config = x))
        .text(
          "Path to application configuration file.  " +
            "This can be any valid HOCON file, typically with .conf suffix (see https://github.com/lightbend/config)"
        )

      Command.CommandEnum.values.foreach(
        command =>
          cmd(command.entryName)
            .action((_, c) => c.copy(command = Some(command.entryName)))
            .text(command.enumEntry.opts.text)
      )

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

  def main(args: Array[String]): Unit = {
    // parser.parse returns Option[C]
    parser.parse(args, Args()) match {
      case Some(args) =>
        getConfig(args.config) match {
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

  case class Args(config: String = "", command: Option[String] = None)

  def run(args: Args, config: Config)(implicit ss: SparkSession): Unit = {
    // Log to stdout (very sparingly) in the event of logging configuration failures
    println(s"Running $progName with args=$args")
    logger.info(s"Running $progName with args=$args")
    logger.debug(s"Configuration to be used for processing=$config")

    args.command match {
      case Some(commandName) => {
        val cmd = Command.CommandEnum.withName(commandName)
        logger.info(s"Running command $cmd")
        cmd.enumEntry.factory(ss, config).run()
      }
      case _ => logger.error("Failed to specify a command to run (try --help)")
    }

    println("Application complete")
  }

}
