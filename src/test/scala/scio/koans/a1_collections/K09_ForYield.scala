package scio.koans.a1_collections

import org.openjdk.jmh.annotations._
import scio.koans.shared._

/**
 * Cartesian product of two lists.
 */
class K09_ForYield extends JmhKoan {
  ImNotDone

  val lhs: List[String] = (1 to 10000).map("lhs-" + _).toList
  val rhs: List[String] = (1 to 5).map("rhs-" + _).toList

  // `for (l <- lhs; r <- rhs) yield (l, r)` expands to
  // `lhs.flatMap(l => rhs.map(r => (l, r)))` which is equivalent to
  // `rhs.map(r => (lhs(0), r)) ++ rhs.map(r => (lhs(1), r)) ++ ... ++ rhs.map(r => (lhs(n), r))`
  // https://www.lyh.me/slides/joins.html
  @Benchmark def baseline: Set[(String, String)] =
    (for {
      l <- lhs
      r <- rhs
    } yield (l, r)).toSet

  /**
   * Hint:
   * - reduce the number of `++` concatenations
   * - output order does not matter as long as it produces the same set of tuples
   */
  @Benchmark def v1: Set[(String, String)] = ???

  verifyResults()
  verifySpeedup(Speedup.Faster)
}
