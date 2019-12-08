package com.relatedsciences.opentargets.etl.schema

import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import java.time.temporal.TemporalAccessor

import scala.util.Try

object Parser {

  val DEFAULT_FORMATS: List[DateTimeFormatter] = List(
    // See https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html
    DateTimeFormatter.ISO_DATE, // 2011-12-03, 2011-12-03+01:00
    DateTimeFormatter.ISO_DATE_TIME, // 2011-12-03T10:15:30, 2011-12-03T10:15:30+01:00, 2011-12-03T10:15:30+01:00[Europe/Paris]
    DateTimeFormatter.ISO_INSTANT // 2011-12-03T10:15:30, 2011-12-03T10:15:30Z, 2018-11-19T00:00:00.000Z, 2018-11-19T00:00:00.0Z
  )

  class DateParser(val formats: Seq[DateTimeFormatter] = DEFAULT_FORMATS) {

    def this(formats: List[String]){
      this(formats.map(DateTimeFormatter.ofPattern))
    }

    private val PARSER = {
      // Build lazy pattern match based on format list to return a value if the date is valid, otherwise nothing
      // Note that while it is possible to use a single DateTimeFormatterBuilder with multiple calls to "appendOptional"
      // patterns, testing shows that is not truly separate evaluations of different formats but rather components
      // added to some single pattern matcher in a way that was found to be very unreliable (e.g. it may work if you
      // specify the optional patterns in one order but not the reverse order)
      def attempt[O](f: String => Option[O]): PartialFunction[String, Option[O]] = {
        case x: String if f(x).isDefined => f(x)
      }
      formats
        .map(f => new DateTimeFormatterBuilder().append(f).toFormatter)
        .map(f => attempt[TemporalAccessor](date => Try(f.parse(date)).toOption))
        .reduce(_ orElse _)
        .orElse[String, Option[TemporalAccessor]]({ case _: String => Option.empty[TemporalAccessor] })
    };

    def parse(value: String): Option[TemporalAccessor] = PARSER.apply(value)
  }
}
