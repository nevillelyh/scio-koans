package scio.koans.a2_coders

import com.spotify.scio.coders.Coder
import scio.koans.shared._

/**
 * Fix the non-deterministic coder exception.
 */
class K03_LatLon2 extends PipelineKoan {
  ImNotDone

  import K03_LatLon2._

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
        .groupByKey
        .mapValues(_.toSet)

      output should containInAnyOrder(expected)
    }
  }
}

object K03_LatLon2 {
  case class LatLon(lat: Double, lon: Double)

  object LatLon {
    // Instead of changing the definition of `LatLon`, define a custom coder
    // Companion object of `LatLon` is searched for implicit `F[LatLon]`, i.e. `Coder[LatLon]`
    // https://docs.scala-lang.org/tutorials/FAQ/finding-implicits.html
    // Hint: derive a `LatLon` coder from a type with deterministic encoding
    implicit val latLonCoder: Coder[LatLon] = Coder.xmap(???)(???, ???)
  }
}
