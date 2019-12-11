package com.relatedsciences.opentargets.etl.pipeline

import com.relatedsciences.opentargets.etl.Utilities
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, struct}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Encoders, Row}

object SparkImplicits {

  implicit class DataFrameOps(df: DataFrame) {

    /**
      * Mutate existing field values using an arbitrary function via recursive schema traversal.
      *
      * Note that this also works for deeply nested struct values and is very useful
      * for updating a small number of them in a large, complicated struct (otherwise
      * redefining the struct manually may better if it is small).
      *
      * @param fn function to apply to each column; should return the column itself if no update desired
      * @example
      *          // Multiply int value by 10 within 2 level struct {contact: person: {age: int}}}
      *          df.mutate(c => if (c.toString == "contact.person.age") c * 10 else c)
      */
    def mutate(fn: Column => Column): DataFrame = {
      // Pass encoder explicitly, per syntactic expansion on context bound to Encoder in applyToDataset
      // See: https://stackoverflow.com/questions/40692691/how-to-pass-encoder-as-parameter-to-dataframes-as-method
      // * Compilation kept failing with unfound encoder regardless of ss.implicits or df.sparkSession.implicits import
      val encoder: ExpressionEncoder[Row] = RowEncoder(df.schema)
      Utilities.applyToDataset[Row](df, fn)(encoder)
    }

    /**
      * Overload on [[mutate]] to apply one function to multiple columns
      *
      * @param columns column paths (possibly nested, e.g. Set("person.age")) to apply mutation to
      * @param fn function to apply to each column provided
      */
    def mutateColumns(columns: String*)(fn: Column => Column): DataFrame = {
      val cols = columns.toSet
      mutate(c => if (cols.contains(c.toString)) fn(c) else c)
    }

    /**
      * Add a string prefix to column names
      * @param prefix string to add to beginning of column names
      * @param columns list of columns to add prefix to; if none, all names will be prefixed
      */
    def addPrefix(prefix: String, columns: Option[Seq[String]] = None): DataFrame = {
      val filter = columns.getOrElse(Set()).toSet
      val cols = df.columns.map(
        c => if (filter.isEmpty || filter.contains(c)) df(c).as(s"$prefix$c") else df(c)
      )
      df.select(cols: _*)
    }

    /**
      * Move fields into a struct and remove from top-level
      * @param name name of resulting struct
      * @param cols columns to move
      */
    def withStruct(name: String, cols: String*): DataFrame = {
      val other = df.columns.filter(c => !cols.contains(c)).map(col).toSeq
      val inner = cols.map(c => df(c))
      df.select(other :+ struct(inner: _*).as(name): _*).drop(cols: _*)
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
