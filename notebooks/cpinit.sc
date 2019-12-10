import java.nio.file.Paths
val jar = java.nio.file.FileSystems
  .getDefault()
  .getPath(
    Paths.get(System.getProperty("user.home"), "data", "ot", "apps", "ot-scoring.jar").toString
  )
val path = ammonite.ops.Path(jar)
interp.load.cp(path)
