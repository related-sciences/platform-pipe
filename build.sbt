name := "ot-scoring"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.1",
  "org.apache.spark" %% "spark-sql" % "2.4.1",
  "org.apache.spark" %% "spark-mllib" % "2.4.1"
)

libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3"