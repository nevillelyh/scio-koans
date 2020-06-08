package scio.koans.b2_cogbk

import com.spotify.scio.values.{SCollection, SideInput}
import scio.koans.shared._

/**
 * Eliminate expensive shuffle with `SideInput`.
 */
class K06_SideInput2 extends TransformKoan {
  ImNotDone

  type InT = (SCollection[(String, Int)], SCollection[(Int, String)], SCollection[(String, String)])
  type OutT = (SCollection[(String, String)])

  val lhs: Seq[(String, Int)] = Seq(
    ("a", 1),
    ("a", 1),
    ("a", 1),
    ("a", 2),
    ("a", 2),
    ("a", 3),
    ("b", 4),
    ("b", 5),
    ("c", 6)
  )

  // Numbers to English mapping
  val i2e: Seq[(Int, String)] = Seq(
    (1, "one"),
    (2, "two"),
    (3, "three"),
    (4, "four"),
    (5, "five"),
    (6, "six")
  )

  // English to Spanish mapping
  val e2s: Seq[(String, String)] = Seq(
    ("one", "uno"),
    ("two", "dos"),
    ("three", "tres"),
    ("four", "cuatro"),
    ("five", "cinco"),
    ("six", "seis")
  )

  // Map integer values to English, then Spanish
  val expected: Seq[(String, String)] = Seq(
    ("a", "uno"),
    ("a", "uno"),
    ("a", "uno"),
    ("a", "dos"),
    ("a", "dos"),
    ("a", "tres"),
    ("b", "cuatro"),
    ("b", "cinco"),
    ("c", "seis")
  )

  prepare(sc => (sc.parallelize(lhs), sc.parallelize(i2e), sc.parallelize(e2s)))
  verify(_ should containInAnyOrder(expected))

  baseline {
    case (lhs, i2e, e2s) =>
      // 2 shuffles from 2 `join`s
      lhs
        .map { case (k, v) => (v, k) }
        .join(i2e)
        .values
        .map {
          case (a, en) =>
            (en, a)
        }
        .join(e2s)
        .values
  }

  test("v1") {
    case (lhs, i2e, e2s) =>
      // 2 `SideInput`s, integer to English and English to Spanish
      val sideInt2En: SideInput[Map[Int, String]] = ???
      val sideEn2Es: SideInput[Map[String, String]] = ???
      lhs
        .withSideInputs(sideInt2En, sideEn2Es)
        .map {
          case ((k, v), ctx) =>
            // Map values to Spanish
            ?:[(String, String)]
        }
        .toSCollection
  }

  test("v2") {
    case (lhs, i2e, e2s) =>
      // Build a single `SideInput` of integer to Spanish to reduce map lookup cost later
      val sideInt2Es: SideInput[Map[Int, String]] = ???
      lhs
        .withSideInputs(sideInt2Es)
        .map {
          case ((k, v), ctx) =>
            // Map values to Spanish
            ?:[(String, String)]
        }
        .toSCollection
  }
}
