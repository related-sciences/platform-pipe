import java.nio.file.Paths
lazy val HOME              = System.getProperty("user.home")
lazy val DATA_DIR          = Paths.get(HOME, "data", "ot")
lazy val EXTRACT_DIR       = DATA_DIR.resolve("extract")
lazy val REPO_DIR          = Paths.get(HOME, "repos", "ot-scoring")
lazy val TEST_RESOURCE_DIR = REPO_DIR.resolve(Paths.get("src", "test", "resources"))
lazy val TEST_PIPELINE_DIR = TEST_RESOURCE_DIR.resolve("pipeline_test")
