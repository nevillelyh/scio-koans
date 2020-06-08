package scio.koans.b2_cogbk

import com.spotify.scio.values.SCollection
import scio.koans.shared._

/**
 * Reduce shuffles from joins and `groupByKey`.
 */
class K04_GroupByKey extends TransformKoan {
  ImNotDone

  type InT = (SCollection[(String, Int)], SCollection[(String, Int)])
  type OutT = (SCollection[(String, Set[Int])])

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

  val rhs: Seq[(String, Int)] = Seq(
    ("a", 3),
    ("b", 5)
  )

  // Build (key -> value set) from LHS and filter out values from RHS
  val expected: Seq[(String, Set[Int])] = Seq(
    ("a", Set(1, 2)),
    ("b", Set(4)),
    ("c", Set(6))
  )

  prepare(sc => (sc.parallelize(lhs), sc.parallelize(rhs)))
  verify(_ should containInAnyOrder(expected))

  baseline {
    case (lhs, rhs) =>
      // 3 shuffles total, 2 from each `.groupByKey`, one from `join`
      lhs.groupByKey
        .fullOuterJoin(rhs.groupByKey)
        .mapValues {
          case (lv, rv) =>
            // Convert `Option[Iterable[Int]]`s to `Set[Int]`s
            lv.toSet.flatten -- rv.toSet.flatten
        }
  }

  test("v1") {
    case (lhs, rhs) =>
      // Reduce the number of shuffles to 2, one from `fullOuterJoin` and one from `groupByKey`
      // However this is convoluted, Cartesian product of the values in `fullOuterJoin` and
      // `groupByKey` again
      lhs.fullOuterJoin(rhs).groupByKey.mapValues { pairs =>
        // Convert `Iterable[(Option[Int], Option[Int])]` to `Set[Int]`s
        pairs.flatMap(_._1).toSet -- pairs.flatMap(_._2).toSet
      }
  }

  test("v2") {
    case (lhs, rhs) =>
      // Only 1 shuffle from `.cogroup`
      lhs.cogroup(rhs).mapValues {
        case (lv, rv) =>
          // Hint: map 2 `Iterable[Int]`s to `Set[Int]`
          ???
      }
  }
}
