package scio.koans.b2_cogbk

import com.spotify.scio.values.SCollection
import scio.koans.shared._

/**
 * Implement `join` with `cogroup`.
 */
class K00_CoGroup extends TransformKoan {
  ImNotDone

  type InT = (SCollection[(String, Int)], SCollection[(String, String)])
  type OutT = (SCollection[(String, (Int, String))])

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

  val expected: Seq[(String, (Int, String))] = Seq(
    ("a", (1, "x")),
    ("a", (1, "y")),
    ("a", (1, "x")),
    ("a", (1, "y")),
    ("a", (1, "x")),
    ("a", (1, "y")),
    ("a", (2, "x")),
    ("a", (2, "y")),
    ("a", (2, "x")),
    ("a", (2, "y")),
    ("a", (3, "x")),
    ("a", (3, "y")),
    ("b", (4, "z")),
    ("b", (5, "z"))
  )

  prepare(sc => (sc.parallelize(lhs), sc.parallelize(rhs)))
  verify(_ should containInAnyOrder(expected))

  baseline { case (lhs, rhs) =>
    // Join is a special case of `cogroup`
    lhs.join(rhs)
  }

  test("v1") { case (lhs, rhs) =>
    lhs.cogroup(rhs).flatMapValues { case (lv, rv) =>
      // Implement Cartesian product of LHS * RHS values
      /** Hint: remember [[scio.koans.a1_collections.K09_ForYield]]? */
      ???
    }
  }
}
