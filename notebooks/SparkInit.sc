import $ivy.`org.apache.spark::spark-sql:2.4.4`
import org.apache.spark.sql._
import org.apache.log4j.{Level, Logger}
Logger.getLogger("org").setLevel(Level.WARN)

val ss = {
  NotebookSparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()
}
