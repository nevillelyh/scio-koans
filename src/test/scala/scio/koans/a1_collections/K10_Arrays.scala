package scio.koans.a1_collections

import org.openjdk.jmh.annotations._
import scio.koans.shared._

/**
 * Compute the dot product of 2 vectors using primitive arrays vs. generic collections
 */
class K10_Arrays extends JmhKoan {
  ImNotDone

  val seq1: Seq[Double] = (1 to 100).map(_.toDouble / 100)
  val seq2: Seq[Double] = (-100 to -1).map(_.toDouble / 100)

  @Benchmark def baseline: Double = {
    var dp = 0.0
    var i = 0
    while (i < seq1.length) {
      dp += seq1(i) * seq2(i)
      i += 1
    }
    dp
  }

  /**
   * - `Seq[Double]` requires boxed `java.lang.Double` due to generics
   * - `Array[Double]` is equivalent to Java primitive array `double[]`
   *
   * - http://www.lyh.me/slides/primitives.html
   */
  val array1: Array[Double] = ???
  val array2: Array[Double] = ???

  @Benchmark def v1: Double = {
    var dp = 0.0
    var i = 0
    while (i < array1.length) {
      dp += array1(i) * array2(i)
      i += 1
    }
    dp
  }

  verifyResults()
  verifySpeedup(Speedup.Times(5))
}
