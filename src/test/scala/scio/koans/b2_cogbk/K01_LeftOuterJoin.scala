package scio.koans.b2_cogbk

import com.spotify.scio.values.SCollection
import scio.koans.shared._

/**
 * Implement `leftOuterJoin` with `cogroup`.
 */
class K01_LeftOuterJoin extends TransformKoan {
  ImNotDone

  type InT = (SCollection[(String, Int)], SCollection[(String, String)])
  type OutT = (SCollection[(String, (Int, Option[String]))])

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

  val rhs: Seq[(String, String)] = Seq(
    ("a", "x"),
    ("a", "y"),
    ("b", "z")
  )

  val expected: Seq[(String, (Int, Option[String]))] = Seq(
    ("a", (1, Some("x"))),
    ("a", (1, Some("y"))),
    ("a", (1, Some("x"))),
    ("a", (1, Some("y"))),
    ("a", (1, Some("x"))),
    ("a", (1, Some("y"))),
    ("a", (2, Some("x"))),
    ("a", (2, Some("y"))),
    ("a", (2, Some("x"))),
    ("a", (2, Some("y"))),
    ("a", (3, Some("x"))),
    ("a", (3, Some("y"))),
    ("b", (4, Some("z"))),
    ("b", (5, Some("z"))),
    ("c", (6, None))
  )

  prepare(sc => (sc.parallelize(lhs), sc.parallelize(rhs)))
  verify(_ should containInAnyOrder(expected))

  baseline {
    case (lhs, rhs) =>
      lhs.leftOuterJoin(rhs)
  }

  test("v1") {
    case (lhs, rhs) =>
      lhs.cogroup(rhs).flatMapValues {
        case (lv, rv) =>
          // Hint: produce a single `None` if `rv` is empty
          ???
      }
  }
}
