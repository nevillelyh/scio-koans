package scio.koans.a1_collections

import org.openjdk.jmh.annotations._
import scio.koans.shared._

import scala.collection.mutable

/**
 * Element-wise sum of a collection vectors, each scaled with a weight.
 */
class K07_WeightedVectors extends JmhKoan {
  ImNotDone

  // 20 vectors of dimension 100.
  val vecs: Seq[(Array[Double], Double)] = (1 to 20).map { x =>
    val vec = (x until x + 100).map(_.toDouble / 200).toArray
    val weight = x.toDouble / (1 to 20).sum
    (vec, weight)
  }

  @Benchmark def baseline: mutable.WrappedArray[Double] =
    vecs
      .map { case (v, w) => v.map(_ * w) }
      .reduce((v1, v2) => (v1 zip v2).map(t => t._1 + t._2))

  @Benchmark def v1: mutable.WrappedArray[Double] = {
    val sum = Array.fill(100)(0.0)
    vecs.foreach {
      case (v, w) =>
        ???
    }
    sum
  }

  verifyResults()
  verifySpeedup(Speedup.Times(100))
}
