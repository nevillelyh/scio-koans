package scio.koans.b2_cogbk

import com.spotify.scio.values.{SCollection, SideInput}
import scio.koans.shared._

/**
 * Eliminate expensive shuffle with `SideInput`.
 */
class K05_SideInput1 extends TransformKoan {
  ImNotDone

  type InT = (SCollection[(String, Int)], SCollection[(String, Int)])
  type OutT = (SCollection[(String, Int)])

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

  // De-duplicate LHS by removing key-value pairs that exist in RHS
  val expected: Seq[(String, Int)] = Seq(
    ("a", 1),
    ("a", 1),
    ("a", 1),
    ("a", 2),
    ("a", 2),
    ("b", 4),
    ("c", 6)
  )

  prepare(sc => (sc.parallelize(lhs), sc.parallelize(rhs)))
  verify(_ should containInAnyOrder(expected))

  baseline { case (lhs, rhs) =>
    val side: SideInput[Seq[(String, Int)]] = rhs.asListSideInput
    lhs
      .withSideInputs(side)
      .filter { case ((k, v), ctx) =>
        // Inefficient, eager `groupBy/mapValues/map/toSet` on the same RHS for every LHS pair
        val sideMap: Map[String, Set[Int]] =
          ctx(side).groupBy(_._1).mapValues(_.map(_._2).toSet)
        !sideMap.getOrElse(k, Set.empty).contains(v)
      }
      .toSCollection
  }

  test("v1") { case (lhs, rhs) =>
    // Prepare data into the desired form before making side input
    // `groupByKey` triggers a shuffle but not an issue assuming RHS is tiny
    val side: SideInput[Map[String, Set[Int]]] =
      rhs.groupByKey.mapValues(_.toSet).asMapSingletonSideInput
    lhs
      .withSideInputs(side)
      .filter { case ((k, v), ctx) =>
        // Side input already prepared, no redundant conversion
        val sideMap: Map[String, Set[Int]] = ???
        !sideMap.getOrElse(k, Set.empty).contains(v)
      }
      .toSCollection
  }

  test("v2") { case (lhs, rhs) =>
    // Prepare data into the desired form before making side input
    // Hint: `.asMapSingletonSideInput` produces `SideInput[Map[K, V]]`, we want `Set[T]`
    val side: SideInput[Set[(String, Int)]] = ???
    lhs
      .withSideInputs(side)
      .filter { case ((k, v), ctx) =>
        // Side input already prepared, no redundant conversion
        val sideSet: Set[(String, Int)] = ???
        ???
      }
      .toSCollection
  }
}
