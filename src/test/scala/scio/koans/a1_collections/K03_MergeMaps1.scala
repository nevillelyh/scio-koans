package scio.koans.a1_collections

import org.openjdk.jmh.annotations.Benchmark
import scio.koans.shared._

/**
 * Merge 2 `Map[String, Set[Int]]`s.
 */
class K03_MergeMaps1 extends JmhKoan {
  ImNotDone

  val map1: Map[String, Set[Int]] = Map(
    "a" -> (1 to 20).toSet,
    "b" -> (1 to 40).toSet,
    "c" -> (1 to 60).toSet,
    "d" -> (1 to 80).toSet,
    "e" -> (1 to 100).toSet
  )

  val map2: Map[String, Set[Int]] = Map(
    "b" -> (21 to 120).toSet,
    "c" -> (41 to 140).toSet,
    "d" -> (61 to 160).toSet,
    "e" -> (81 to 180).toSet,
    "f" -> (101 to 200).toSet
  )

  @Benchmark def baseline: Map[String, Set[Int]] =
    (map1.keySet ++ map2.keySet)
      .map(k => k -> (map1.getOrElse(k, Set.empty) ++ map2.getOrElse(k, Set.empty)))
      .toMap

  // Why is this faster than `baseline`?
  @Benchmark def v1: Map[String, Set[Int]] = {
    val commonKeys = map1.keySet intersect map2.keySet
    val common = commonKeys.map(k => k -> (map1(k) ++ map2(k))).toMap
    (map1 -- commonKeys) ++ (map2 -- commonKeys) ++ common
  }

  // Hint: if `k` exists in both `m1 ++ m2`, value in `m2` wins
  // How much faster is this version?
  @Benchmark def v2: Map[String, Set[Int]] =
    map1 ++ map2.map(kv => ???)

  verifyResults()
  verifySpeedup(Speedup.Times(2))
}
