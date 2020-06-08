package scio.koans.b2_cogbk

import com.spotify.scio.values.SCollection
import scio.koans.shared._

/**
 * Implement `fullOuterJoin` with `cogroup`.
 */
class K03_FullOuterJoin extends TransformKoan {
  ImNotDone

  type InT = (SCollection[(String, Int)], SCollection[(String, String)])
  type OutT = (SCollection[(String, (Option[Int], Option[String]))])

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
    ("b", "z"),
    ("d", "x")
  )

  val expected: Seq[(String, (Option[Int], Option[String]))] = Seq(
    ("a", (Some(1), Some("x"))),
    ("a", (Some(1), Some("y"))),
    ("a", (Some(1), Some("x"))),
    ("a", (Some(1), Some("y"))),
    ("a", (Some(1), Some("x"))),
    ("a", (Some(1), Some("y"))),
    ("a", (Some(2), Some("x"))),
    ("a", (Some(2), Some("y"))),
    ("a", (Some(2), Some("x"))),
    ("a", (Some(2), Some("y"))),
    ("a", (Some(3), Some("x"))),
    ("a", (Some(3), Some("y"))),
    ("b", (Some(4), Some("z"))),
    ("b", (Some(5), Some("z"))),
    ("c", (Some(6), None)),
    ("d", (None, Some("x")))
  )

  prepare(sc => (sc.parallelize(lhs), sc.parallelize(rhs)))
  verify(_ should containInAnyOrder(expected))

  baseline {
    case (lhs, rhs) =>
      // Join is a special case of `cogroup`
      lhs.fullOuterJoin(rhs)
  }

  test("v1") {
    case (lhs, rhs) =>
      lhs.cogroup(rhs).flatMapValues {
        case (lv, rv) =>
          // Produce a `None` if either `lv` or `rv` is empty
          ???
      }
  }
}
