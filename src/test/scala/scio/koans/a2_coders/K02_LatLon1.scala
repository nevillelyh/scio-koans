package scio.koans.a2_coders

import scio.koans.shared._

/**
 * Fix the non-deterministic coder exception.
 */
class K02_LatLon1 extends PipelineKoan {
  ImNotDone

  import K02_LatLon1._

  val events: Seq[(LatLon, String)] = Seq(
    (LatLon(34.5, -10.0), "a"),
    (LatLon(67.8, 12.3), "b"),
    (LatLon(67.8, 12.3), "c"),
    (LatLon(-45.0, 3.14), "d")
  )

  val expected: Seq[(LatLon, Set[String])] = Seq(
    (LatLon(34.5, -10.0), Set("a")),
    (LatLon(67.8, 12.3), Set("b", "c")),
    (LatLon(-45.0, 3.14), Set("d"))
  )

  "Snippet" should "work" in {
    runWithContext { sc =>
      val output = sc
        .parallelize(events)
        // `*ByKey` transforms compare keys using encoded bytes and must be deterministic
        .groupByKey
        .mapValues(_.toSet)

      output should containInAnyOrder(expected)
    }
  }
}

object K02_LatLon1 {
  // Latitude and longitude are degrees bounded by [-90, 90] and [-180, 180] respectively.
  // 1 degree = 60 minutes
  // 1 minute = 60 seconds
  // https://en.wikipedia.org/wiki/Decimal_degrees#Precision
  // Hint: `Double` encoding is not deterministic but `Int` is
  case class LatLon(lat: Double, lon: Double)
}
