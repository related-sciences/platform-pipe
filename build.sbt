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
