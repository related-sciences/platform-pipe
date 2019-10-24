/**
 * Scoring pipeline execution for spark-shell
 * Usage:
 * /usr/spark-2.4.1/bin/spark-shell --driver-memory 12g --jars target/scala-2.11/ot-scoring_2.11-0.1.jar -i scripts/scoring_pipeline_exec.scala
 */
import com.relatedsciences.opentargets.pipeline.Pipeline
new Pipeline(spark).execute()
System.exit(0)