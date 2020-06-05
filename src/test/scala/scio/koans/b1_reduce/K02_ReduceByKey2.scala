package scio.koans.b1_reduce

import com.spotify.scio.values.SCollection
import scio.koans.shared._

/**
 * Replace `groupByKey` with `reduceByKey`.
 */
class K02_ReduceByKey2 extends TransformKoan {
  ImNotDone

  type InT = SCollection[(String, Int)]
  type OutT = SCollection[(String, (Int, Int))]

  val keys = Seq("a", "b", "c", "d", "e")
  val input: Seq[(String, Int)] = for {
    k <- keys
    v <- 1 to 100
  } yield k -> v
  val expected: Seq[(String, (Int, Int))] = keys.map(_ -> (100, 5050))

  prepare(_.parallelize(input))
  verify(_ should containInAnyOrder(expected))

  /**
   * - `.groupByKey` shuffles everything
   * - `xs.size` and `xs.sum` each loops over all items
   */
  baseline {
    _.groupByKey.mapValues(xs => (xs.size, xs.sum))
  }

  // Hint: count and sum at the same time over `(Int, Int)`
  test("v1") {
    _.mapValues(v => ?:[(Int, Int)]).reduceByKey(???)
  }
}
