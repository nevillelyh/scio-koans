package scio.koans.a2_coders

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import scio.koans.shared._

/**
 * Fix the `NullPointerException`.
 */
class K01_LocalDate extends PipelineKoan {
  ImNotDone

  val events: Seq[(String, String)] = Seq(
    ("2020-01-01", "a"),
    ("2020-01-02", "a"),
    ("2020-02-01", "b"),
    ("2020-02-02", "c"),
    ("2020-03-01", "c"),
    ("2020-03-03", "d"),
    ("BAD DATE", "x")
  )

  val expected: Seq[((Int, Int), Set[String])] = Seq(
    ((2020, 1), Set("a")),
    ((2020, 2), Set("b", "c")),
    ((2020, 3), Set("c", "d")),
    (null, Set("x"))
  )

  "Snippet" should "work" in {
    runWithContext { sc =>
      import K01_LocalDate.formatter

      val output = sc
        .parallelize(events)
        .map {
          case (dateStr, event) =>
            // Hint: avoid null by emitting something else in case of exception
            try {
              val date = LocalDate.from(formatter.parse(dateStr))
              ((date.getYear, date.getMonth.getValue), event)
            } catch {
              case _: Throwable => (null, event)
            }
        }
        .groupByKey
        .mapValues(_.toSet)

      output should containInAnyOrder(expected)
    }
  }
}

object K01_LocalDate {
  val formatter: DateTimeFormatter = DateTimeFormatter.ISO_LOCAL_DATE
}
