package scio.koans.a1_collections

import org.openjdk.jmh.annotations.Benchmark
import scio.koans.shared._

import scala.collection.mutable

/**
 * Merge multiple `Map[String, Set[Int]]`s.
 */
class K04_MergeMaps2 extends JmhKoan {
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

  val map3: Map[String, Set[Int]] = Map(
    "c" -> (101 to 120).toSet,
    "d" -> (121 to 140).toSet,
    "e" -> (141 to 160).toSet,
    "f" -> (161 to 180).toSet,
    "g" -> (181 to 200).toSet
  )

  @Benchmark def baseline: Map[String, Set[Int]] =
    (map1.keySet ++ map2.keySet ++ map3.keySet).map { k =>
      val v1 = map1.getOrElse(k, Set.empty)
      val v2 = map2.getOrElse(k, Set.empty)
      val v3 = map3.getOrElse(k, Set.empty)
      (k, v1 ++ v2 ++ v3)
    }.toMap

  // Hint: if `k` exists in both `map1` and `map2`, value in `map2` wins
  @Benchmark def v1: Map[String, Set[Int]] =
    Seq(map1, map2, map3).reduce { (m1, m2) =>
      m1 ++ ???
    }

  // How much faster is this version?
  @Benchmark def v2: mutable.Map[String, Set[Int]] = {
    val map = mutable.Map.empty[String, Set[Int]]
    Seq(map1, map2, map3).foreach { m =>
      for ((k, v) <- m) {
        ???
      }
    }
    map
  }

  verifyResults()
  verifySpeedup(Speedup.Times(2))
}
