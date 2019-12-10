import java.nio.file.Paths
import $file.pathinit, pathinit._
interp.load.cp(ammonite.ops.Path(java.nio.file.FileSystems
  .getDefault()
  .getPath(PROJECT_JAR_PATH.toString)))
