package scio.koans.b2_cogbk

import com.spotify.scio.values.SCollection
import scio.koans.shared._

/**
 * Implement `rightOuterJoin` with `cogroup`.
 */
class K02_RightOuterJoin extends TransformKoan {
  ImNotDone

  type InT = (SCollection[(String, Int)], SCollection[(String, String)])
  type OutT = (SCollection[(String, (Option[Int], String))])

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

  val expected: Seq[(String, (Option[Int], String))] = Seq(
    ("a", (Some(1), "x")),
    ("a", (Some(1), "y")),
    ("a", (Some(1), "x")),
    ("a", (Some(1), "y")),
    ("a", (Some(1), "x")),
    ("a", (Some(1), "y")),
    ("a", (Some(2), "x")),
    ("a", (Some(2), "y")),
    ("a", (Some(2), "x")),
    ("a", (Some(2), "y")),
    ("a", (Some(3), "x")),
    ("a", (Some(3), "y")),
    ("b", (Some(4), "z")),
    ("b", (Some(5), "z")),
    ("d", (None, "x"))
  )

  prepare(sc => (sc.parallelize(lhs), sc.parallelize(rhs)))
  verify(_ should containInAnyOrder(expected))

  baseline { case (lhs, rhs) =>
    lhs.rightOuterJoin(rhs)
  }

  test("v1") { case (lhs, rhs) =>
    lhs.cogroup(rhs).flatMapValues { case (lv, rv) =>
      // Hint: produce a single `None` if `lv` is empty
      ???
    }
  }
}
