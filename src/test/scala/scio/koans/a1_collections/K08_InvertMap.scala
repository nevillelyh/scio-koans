package scio.koans.a1_collections

import org.openjdk.jmh.annotations._
import scio.koans.shared._

import scala.collection.mutable

/**
 * Invert a `Map[String, Set[Int]]` to `Map[Int, Set[String]]`
 */
class K08_InvertMap extends JmhKoan {
  ImNotDone

  val map: Map[String, Set[Int]] =
    Map("a" -> Set(1, 2, 3), "b" -> Set(2, 3, 4), "c" -> Set(4, 5, 6))

  @Benchmark def baseline: Map[Int, Set[String]] =
    map.toList
      .flatMap {
        case (k, v) =>
          v.map(g => (g, k))
      }
      .groupBy(_._1)
      .mapValues(_.map(_._2).toSet)

  @Benchmark def v1: mutable.Map[Int, Set[String]] = {
    val m = mutable.Map.empty[Int, Set[String]]
    ???
    m
  }

  verifyResults()
  verifySpeedup(Speedup.Times(2))
}
