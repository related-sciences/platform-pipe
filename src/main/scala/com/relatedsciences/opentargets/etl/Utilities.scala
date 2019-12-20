package com.relatedsciences.opentargets.etl
import java.net.{URI, URL}

import scala.concurrent.duration.{Duration, NANOSECONDS}
import scala.io.Source
import org.yaml.snakeyaml.Yaml
import java.util.{Map => JMap}

import org.apache.spark.sql.{Column, Dataset, Encoder}
import org.apache.spark.sql.functions.{col, struct, when}
import org.apache.spark.sql.types.StructType

object Utilities {

  /**
    * Resource manager wrapper
    * @param r resource to close after use
    * @param f function to apply to resource
    */
  def using[A](r: Source)(f: Source => A): A = {
    try f(r)
    finally r.close()
  }

  def clip[T](v: T, rng: (T, T))(implicit n: Numeric[T]): T = {
    val x  = n.toDouble(v)
    val r1 = n.toDouble(rng._1)
    val r2 = n.toDouble(rng._2)
    if (x > r2) rng._2
    else if (x < r1) rng._1
    else v
  }

  /**
    * Project a value within one numeric domain to another
    *
    * For example, the value .5 in the domain [0, 1] may be projected into the domain [0, 100] where the
    * corresponding value is 50.
    *
    * @param v value to project
    * @param curRng current domain as (start, end)
    * @param newRng new domain as (start, end)
    * @return value in new domain
    * @throws IllegalArgumentException if start of either range is less than or equal to end of range
    */
  def normalize(v: Double, curRng: (Double, Double), newRng: (Double, Double)): Double = {
    require(curRng._2 > curRng._1)
    require(newRng._2 > newRng._1)
    val r = newRng._1 + ((v - curRng._1) / (curRng._2 - curRng._1)) * (newRng._2 - newRng._1)
    clip(r, newRng)
  }

  /**
    * Convert p value to score
    * @param pValue p value (linear scale, not log)
    * @param rng domain bounds on pValue
    * @return score in [0, 1] with directionality inverted, i.e. a higher score corresponds to a lower p value
    */
  def scorePValue(pValue: Double, rng: (Double, Double) = (1e-10, 1.0)): Double = {
    val pValueLog = Math.log10(pValue)
    val rngLog    = (Math.log10(rng._1), Math.log10(rng._2))
    // Normalize log p value into 0-1 range and flip interpretation since higher p-values are
    // supposed to have lower scores (i.e. scorePValue(.0001) > scorePValue(.1))
    val pLinear = normalize(pValueLog, rngLog, (0.0, 1.0))
    assert(pLinear >= 0 && pLinear <= 1)
    1.0 - pLinear
  }

  /**
    * Walk over the structure of a struct recursively, apply some function to all non-struct columns (which
    * could be arrays or other containers)
    *
    * Inspiration taken from:
    * - https://www.data-engineer.in/bigdata/flatten-dataframes-apache-spark-sql-1/
    * - https://stackoverflow.com/questions/51329926/renaming-columns-recursively-in-a-netsted-structure-in-spark
    *
    * @param df dataset to traverse
    * @param fn function to apply at each column; e.g. { case "contact.person.age" => col("contact.person.age") * 10 }
    * @return
    */
  def applyToDataset[T](df: Dataset[T], fn: PartialFunction[String, Column])(
      implicit enc: Encoder[T]
  ): Dataset[T] = {
    val projection = traverse(df.schema, fn)
    df.sqlContext.createDataFrame(df.select(projection: _*).rdd, enc.schema).as[T]
  }

  /**
    * Construct projection through recursive schema traversal
    * @param schema any struct schema (typically Dataset.schema)
    * @param fn function to apply to each non-struct column within schema
    * @return Array of columns for use in projection
    * @example
    *          val projection = traverse(df.schema, { case "contact.person.age" => col("contact.person.age") * 10 })
    *          df.select(projection:_*) // This will apply the function to nested columns
    */
  def traverse(schema: StructType, fn: PartialFunction[String, Column], path: String = ""): Array[Column] = {
    schema.fields.map(f => {
      val p = path + f.name
      val c = col(p)
      f.dataType match {
        case s: StructType => fn.orElse({ case _ => when(c.isNotNull, struct(traverse(s, fn, p + "."): _*))}: PartialFunction[String, Column])(p)
        case _ => fn.orElse({ case _ => c }: PartialFunction[String, Column])(p)
      }
    })
  }

  class Stopwatch {
    val startedAtNanos: Long = System.nanoTime()

    def elapsed(): Duration = {
      val nanos = (System.nanoTime() - startedAtNanos)
      Duration(nanos, NANOSECONDS)
    }
  }

  object Stopwatch {
    def start(): Stopwatch = new Stopwatch
  }

  def loadYaml(path: String): JMap[String, Any] = {
    loadYaml(Source.fromFile(path))
  }

  def loadYaml(url: URL): JMap[String, Any] = {
    loadYaml(Source.fromFile(url.toURI))
  }

  def loadYaml(source: Source): JMap[String, Any] = {
    val content = Utilities.using(source)(f => f.mkString)
    (new Yaml).load(content).asInstanceOf[JMap[String, Any]]
  }

}
