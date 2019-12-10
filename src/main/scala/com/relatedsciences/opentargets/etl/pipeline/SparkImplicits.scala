package com.relatedsciences.opentargets.etl.pipeline

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, struct}

object SparkImplicits {

  implicit class DataFrameOps(df: DataFrame) {

    /**
    * Add or overwrite a top-level field in a struct
      *
      * This is necessary to avoid redefining all elements of a struct during mutation,
      * however it is limited to the first nesting level.
      * @param structField Name of struct field to mutate
      * @param valueField Name of field within struct to mutate
      * @param value New value for valueField
      * @example df.withStructColumn("person", "age", $"person.age" + 1)
      */
    def withStructColumn(structField: String, valueField: String, value: Column): DataFrame = {
      val values = df
        // Get list of all column names within target struct
        .select(structField + ".*").columns
        // Ignore the name of the inner field to overwrite
        .filter(_ != valueField).toList
        // Define each column with original name
        .map(f => col(structField + "." + f).as(f))
      // Overwrite the entire struct field except for the target field
      df.withColumn(structField, struct((value.as(valueField) :: values): _*))
    }

    /**
    * Add a string prefix to column names
      * @param prefix string to add to beginning of column names
      * @param columns list of columns to add prefix to; if none, all names will be prefixed
      */
    def addPrefix(prefix: String, columns: Option[Seq[String]] = None): DataFrame = {
      val filter = columns.getOrElse(Set()).toSet
      val cols = df.columns.map(c => if (filter.isEmpty || filter.contains(c)) df(c).as(s"$prefix$c") else df(c))
      df.select(cols: _*)
    }

    /**
    * Apply function to self
      * @param f function to apply to self
      */
    def pipe(f: DataFrame => DataFrame): DataFrame = {
      f(df)
    }
  }

}
