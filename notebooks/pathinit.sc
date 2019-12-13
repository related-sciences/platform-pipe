import java.nio.file.Paths
lazy val HOME                     = sys.env.get("HOME").get
lazy val DEFAULT_DATA_DIR         = Paths.get(HOME, "data", "ot").toString
lazy val DATA_DIR                 = Paths.get(sys.env.getOrElse("OT_DATA_DIR", DEFAULT_DATA_DIR))
lazy val EXTRACT_DIR              = DATA_DIR.resolve("extract")
lazy val RESULTS_DIR              = DATA_DIR.resolve("results")
lazy val REPO_DIR                 = Paths.get(HOME, "repos", "ot-scoring")
lazy val TEST_RESOURCE_DIR        = REPO_DIR.resolve(Paths.get("src", "test", "resources"))
lazy val MAIN_RESOURCE_DIR        = REPO_DIR.resolve(Paths.get("src", "main", "resources"))
lazy val TEST_PIPELINE_DIR        = TEST_RESOURCE_DIR.resolve("pipeline_test")
lazy val DEFAULT_PROJECT_JAR_PATH = DATA_DIR.resolve("apps").resolve("ot-scoring.jar").toString
lazy val PROJECT_JAR_PATH =
  Paths.get(sys.env.getOrElse("PROJECT_JAR_PATH", DEFAULT_PROJECT_JAR_PATH))
