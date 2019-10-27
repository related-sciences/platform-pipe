name := "ot-scoring"
organization := "com.relatedsciences"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.1",
  "org.apache.spark" %% "spark-sql" % "2.4.1",
  "org.apache.spark" %% "spark-mllib" % "2.4.1"
)

libraryDependencies += "org.yaml" % "snakeyaml" % "1.21"

libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.8"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % "test"

libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.8"

testOptions in Test += Tests.Argument("-oF")