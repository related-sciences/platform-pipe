import Dependencies._

lazy val root = (project in file("."))
  .settings(
    inThisBuild(
      List(
        organization := "com.relatedsciences",
        version := "latest",
        name := "ot-scoring",
        // Scala version used to build spark in package spark-2.4.4-bin-without-hadoop-scala-2.12
        scalaVersion := "2.12.8"
      )
    ),
    resolvers ++= Seq(
      "jitpack" at "https://jitpack.io"
    ),
    libraryDependencies ++= loggingDeps,
    libraryDependencies ++= sparkDeps,
    libraryDependencies ++= codeDeps,
    libraryDependencies ++= configDeps,
    libraryDependencies ++= testingDeps,

    assemblyJarName in assembly := { "ot-scoring.jar" },
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
    },
    assemblyExcludedJars in assembly := {
      val cp = (fullClasspath in assembly).value
      cp filter { f =>
        f.data.getName.contains("slf4j")
      }
    }
  )

/** NOTE: With multiple JDKs installed on a system, it becomes crucial that sbt chooses a 1.8 JDK install.
  * On mac, this can be ensured with export JAVA_HOME=`/usr/libexec/java_home -v 1.8`.
  * sbt may choose an appropriate JDK based on other build properties, but I've found
  * it doesn't always do that so the explicit JAVA_HOME setting becomes necessary prior to any sbt commands
  * (though it is not an issue with IntelliJ if sbt is configured there to use a specific JDK). */