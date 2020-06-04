package scio.koans.a1_collections

import org.openjdk.jmh.annotations._
import scio.koans.shared._

/**
 * Compute the dot product of 2 vectors.
 */
class K05_DotProduct extends JmhKoan {
  ImNotDone

  val vec1: Array[Double] = (1 to 100).map(_.toDouble / 100).toArray
  val vec2: Array[Double] = (-100 to -1).map(_.toDouble / 100).toArray

  @Benchmark def baseline: Double = (vec1 zip vec2).map(t => t._1 * t._2).sum

  // Why is this faster than `baseline`?
  @Benchmark def v1: Double = (vec1.iterator zip vec2.iterator).map(t => t._1 * t._2).sum

  // How much faster is this version?
  @Benchmark def v2: Double = {
    var sum = 0.0
    var i = 0
    while (i < vec1.length) {
      ???
      i += 1
    }
    sum
  }

  verifyResults()
  verifySpeedup(Speedup.Percent(20))
}
