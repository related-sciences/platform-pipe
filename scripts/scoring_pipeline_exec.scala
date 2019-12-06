/**
 * Scoring pipeline execution for spark-shell
 * Usage:
 * /usr/spark-2.4.4/bin/spark-shell --driver-memory 12g --jars target/scala-2.12/ot-scoring_2.12-0.1.jar -i scripts/scoring_pipeline_exec.scala
 */
import com.relatedsciences.opentargets.etl.PipelineOld
new PipelineOld(spark)
  .runPreprocessing()
  .runScoring()
System.exit(0)