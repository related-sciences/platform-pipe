import sbt._

object Dependencies {

  lazy val loggingDeps = Seq(
    "ch.qos.logback"             % "logback-classic" % "1.2.3",
    "com.typesafe.scala-logging" %% "scala-logging"  % "3.9.0"
  )

  lazy val configDeps = Seq(
    "org.yaml"              % "snakeyaml"   % "1.21",
    "com.github.pureconfig" %% "pureconfig" % "0.12.1",
    "com.github.pureconfig" %% "pureconfig-enumeratum" % "0.8.0"
  )

  lazy val codeDeps = Seq(
    "com.beachape"                      %% "enumeratum"            % "1.5.13",
    "com.github.scopt"                  %% "scopt"                 % "3.7.1",
    "com.github.everit-org.json-schema" % "org.everit.json.schema" % "1.12.0"
  )

  lazy val testingDeps = Seq(
    "org.scalactic" %% "scalactic" % "3.0.8",
    "org.scalatest" %% "scalatest" % "3.0.8" % "test"
  )

  lazy val sparkDeps = Seq(
    "org.apache.spark" %% "spark-core"  % "2.4.4" % "provided" classifier "tests",
    "org.apache.spark" %% "spark-sql"   % "2.4.4" % "provided",
    "org.apache.spark" %% "spark-mllib" % "2.4.4" % "provided"
  )

}
