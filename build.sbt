import Dependencies._
import sbt.util
name := "ot-scoring"
organization := "com.relatedsciences"
version := "0.1"

resolvers ++= Seq(
  "Maven repository" at "https://download.java.net/maven/2/",
  "Typesafe Repo" at "https://repo.typesafe.com/typesafe/releases/",
  "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
  "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases"
)

// Scala version used to build spark in package spark-2.4.4-bin-without-hadoop-scala-2.12
scalaVersion := "2.12.8"

libraryDependencies ++= loggingDeps
libraryDependencies ++= sparkDeps
libraryDependencies ++= configDeps
libraryDependencies ++= testingDeps

assemblyMergeStrategy in assembly := {
  case PathList("org", "aopalliance", xs @ _*)      => MergeStrategy.last
  case PathList("javax", "inject", xs @ _*)         => MergeStrategy.last
  case PathList("javax", "servlet", xs @ _*)        => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*)     => MergeStrategy.last
  case PathList("org", "apache", xs @ _*)           => MergeStrategy.last
  case PathList("com", "google", xs @ _*)           => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _*)         => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*)           => MergeStrategy.last
  case PathList("org", "slf4j", "impl", xs @ _*)    => MergeStrategy.last
  case "about.html"                                 => MergeStrategy.rename
  case "overview.html"                              => MergeStrategy.rename
  case "plugin.properties"                          => MergeStrategy.last
  case "log4j.properties"                           => MergeStrategy.last
  case "git.properties"                             => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
