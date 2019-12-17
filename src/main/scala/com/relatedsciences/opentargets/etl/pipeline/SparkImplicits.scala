package com.relatedsciences.opentargets.etl.pipeline

import com.relatedsciences.opentargets.etl.Utilities
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.functions.{col, struct}
import org.apache.spark.sql.{Column, DataFrame, Row}

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

    /** Move top-level columns into a struct */
    private def createStruct(name: String, cols: String*)(df: DataFrame): DataFrame = {
      val other = df.columns.filter(c => !cols.contains(c)).map(col).toSeq
      val inner = cols.map(c => df(c).as(c))
      df.select(other :+ struct(inner: _*).as(name): _*).drop(cols: _*)
    }

    /**
      * Move top-level fields into a struct
      *
      * The struct will be created if it does not already exist or if it does, the fields
      * will be merged into it.  In all cases, the original fields to append are dropped.
      *
      * @param structName name of resulting struct
      * @param cols columns to move
      */
    def appendStruct(structName: String, cols: String*): DataFrame = {
      if (df.columns.contains(structName)) {
        // If the struct exists, create a temporary one with the desired columns and merge to original
        val tmpName = "__" + structName + "__"
        assert(!df.columns.contains(tmpName))
        df.transform(createStruct(tmpName, cols: _*))
          .withColumn(structName, struct(col(structName + ".*"), col(tmpName + ".*")))
          .drop(tmpName)
      } else {
        // If the struct does not exist, pack the fields into it and remove the old ones
        df.transform(createStruct(structName, cols: _*))
      }
    }

  }

}
