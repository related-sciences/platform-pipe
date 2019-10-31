/**
    * Scoring pipeline execution for spark-shell
    * Usage:
    * /usr/spark-2.4.1/bin/spark-shell --driver-memory 12g --jars target/scala-2.11/ot-scoring_2.11-0.1.jar -i scripts/create_test_datasets.scala
    */
import java.nio.file.{Files, Paths, StandardCopyOption}

import com.relatedsciences.opentargets.pipeline.Configuration
import com.relatedsciences.opentargets.pipeline.schema.{DataType, Fields}
import com.relatedsciences.opentargets.pipeline.scoring.Scoring
import org.apache.spark.sql.functions.{col, explode, udf}
import org.apache.spark.sql._

import scala.reflect.io.Directory

val config = Configuration.default()

// This is redundant in local mode with spark-shell but it makes IDE auto-builds
// much more useful when assumed declared rather than implicit
val spark = SparkSession
  .builder()
  .master("local[*]")
  .appName("test_data_extraction")
  .config("spark.sql.shuffle.partitions", "1")
  .config("spark.ui.enabled", "false")
  .getOrCreate()
import spark.implicits._

// Path within container to test resource folder
val testResourceDir =
  Paths.get(System.getProperty("user.home"), "repos", "ot-scoring", "src", "test", "resources")

/* Load raw evidence data with union of fields necessary for scoring */
val primaryCols = List(
  ("id", "id"),
  ("target.id", "target_id"),
  ("disease.id", "disease_id"),
  ("type", "type_id"),
  ("sourceID", "source_id"),
  ("scores.association_score", "expected_score")
)
val resourceCols = Fields.allColumns.toList

// Collectively, these genes have at least one record for each data type
val genes = List(
  "ENSG00000141510", // TP53
  "ENSG00000169174", // PCSK9
  "ENSG00000105397" // TYK2
)

// Collect evidence data for specified genes
def getEvidenceData: DataFrame = {
  spark.read
    .json(config.inputPath.resolve("evidence.json").toString)
    .filter($"target.id".isin(genes: _*))
}
var df = getEvidenceData

def project(dataType: DataType.Value)(df: DataFrame): DataFrame = {
  // Project out only top-level identifiers and fields necessary for scoring the data type
  val scorer = Scoring.Scorer.byTypeId(dataType.toString).get
  val cols = primaryCols.map(f => col(f._1).as(f._2)) ++ Fields.toColumns(scorer.fields)
    //scorer.fields.map(f => col(FieldName.pathName(f)).as(FieldName.flatName(f)))
  df.select(cols: _*)
}

def saveDataset(writer: DataFrameWriter[Row], path: String, ext: String) = {
  val tempDir = path + ".tmp"

  // Write the data to a temporary directory
  writer.save(tempDir)

  // Find the single file written to the directory above (which will have partitioned named with guid)
  val files = Paths.get(tempDir).toFile.listFiles().filter(_.getName.endsWith("." + ext))
  if (files.length != 1) {
    throw new IllegalStateException(s"Failed to find single file in result dir $tempDir")
  }

  // Copy the file to the original output location and remove the temporary directory
  Files.copy(files(0).toPath, Paths.get(path), StandardCopyOption.REPLACE_EXISTING)
  new Directory(new java.io.File(tempDir)).deleteRecursively()
}

def saveScoringTestData(df: DataFrame, dataType: DataType.Value): String = {
  val path   = testResourceDir.resolve("scorer_test").resolve(s"$dataType.json").toString
  val writer = df.coalesce(1).write.mode(SaveMode.Overwrite).format("json")
  saveDataset(writer, path, "json")
  path
}

/* ---------------------------- */
/* Scoring Test Data Collection */
/* ---------------------------- */

// Extract and save sample data for each data type
DataType.values.toList.foreach { t =>
  println(s"Creating test data for data type '$t'")
  // Select out only columns and rows necessary for this data type
  // as well as limit to a random 50 records
  val dft = df
    .filter($"type" === t.toString)
    .transform(project(t))
    .sample(false, 1.0, seed = 1)
    .limit(50)
  if (!dft.isEmpty) {
    println("First 10 rows:")
    dft.show(10, 12)
    val path = saveScoringTestData(dft, t)
    println(s"Result saved to '$path'")
  } else {
    println(s"Skipping data type '$t' because it has no data")
  }
}

/* ----------------------------- */
/* Pipeline Test Data Collection */
/* ----------------------------- */

def savePipelineTestData(df: DataFrame, filename: String, compress: Boolean): Boolean = {
  var path = testResourceDir
    .resolve("pipeline_test")
    .resolve("input")
    .resolve(s"$filename.json").toString
  var writer = df.coalesce(1)
    .write
    .format("json")
    .mode(SaveMode.Overwrite)
  var extension = "json"

  // Add compression options if needed
  if (compress){
    extension = "json.gz"
    path += ".gz"
    writer = writer.option("compression", "gzip")
  }

  println(s"Saving $filename data for full pipeline test to $path")
  saveDataset(writer, path, extension)
}

/* Aggregated scores for comparison */

def getAssocationData: DataFrame = {
  spark.read
    .json(config.inputPath.resolve("association.json").toString)
    .filter($"target.id".isin(genes: _*))
}

var dfa = getAssocationData

/* Assocation level aggregates */
val dfao = dfa.select(
  $"target.id".as("target_id"),
  $"disease.id".as("disease_id"),
  $"harmonic-sum.overall".as("score")
)

/* Source level aggregates */
val to_map = udf((r: Row) => {
  r.getValuesMap[Double](r.schema.fieldNames)
})
val dfas = dfa
  .select(
    $"target.id".as("target_id"),
    $"disease.id".as("disease_id"),
    explode(to_map($"harmonic-sum.datasources"))
  )
  .withColumnRenamed("key", "source_id")
  .withColumnRenamed("value", "score")
  // Exploding the struct of non-nullable doubles
  // results in a bunch of zeros that can be ignored
  .filter($"score" > 0)

// Save all data necessary for pipeline test
savePipelineTestData(df, "evidence", compress = true)
savePipelineTestData(dfao, "association_scores", compress = false)
savePipelineTestData(dfas, "source_scores", compress = false)

println("Data sample extraction complete")
System.exit(0)