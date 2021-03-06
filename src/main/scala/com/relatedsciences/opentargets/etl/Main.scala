package com.relatedsciences.opentargets.etl
import java.nio.file.Paths
import java.text.SimpleDateFormat
import java.util.Date

import com.relatedsciences.opentargets.etl.configuration.Configuration.Config
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession
import scopt.OptionParser
// Do not remove any of these -- while flagged as unused in IntelliJ, they are necessary for config parsing
import pureconfig._
import pureconfig.generic.auto._
import pureconfig.module.enumeratum._

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

  private def getConfig(path: String): Config = { //implicit reader used to read the config file
    if (path.nonEmpty) {
      logger.info(s"Loading configuration from path '$path'")
      ConfigSource.file(Paths.get(path)).loadOrThrow[Config]
    } else {
      logger.info("Loading configuration from packaged application (path not provided)")
      ConfigSource.default.loadOrThrow[Config]
    }
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

      note(entryText)

      override def showUsageOnError = true
    }
  }

  def main(args: Array[String]): Unit = {
    parser.parse(args, Args()) match {
      case Some(args) => {
        val config = getConfig(args.config)
        implicit val ss: SparkSession = getSparkSession(config)
        ss.sparkContext.setLogLevel(config.logLevel)
        try {
          run(args, config)
        } finally {
          ss.close()
        }
      }
      case None => throw new IllegalArgumentException(
        "Failed to parse command line arguments")
    }
  }

  def now: String = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss").format(new Date)
  case class Args(config: String = "", command: Option[String] = None)

  def run(args: Args, config: Config)(implicit ss: SparkSession): Unit = {
    // Log to stdout (very sparingly) in the event of logging configuration failures
    println(s"Running $progName with args=$args ($now)")
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

    logger.info(s"Application complete")
    println(s"Application complete ($now)")
  }

}
