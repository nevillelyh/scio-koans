package scio.koans.b1_reduce

import com.spotify.scio.values.SCollection
import scio.koans.shared._

/**
 * Replace `groupByKey` with `reduceByKey`.
 */
class K01_ReduceByKey1 extends TransformKoan {
  ImNotDone

  type InT = SCollection[(String, Int)]
  type OutT = SCollection[(String, Int)]

  val keys = Seq("a", "b", "c", "d", "e")
  val input: Seq[(String, Int)] = for {
    k <- keys
    v <- 1 to 100
  } yield k -> v
  val expected: Seq[(String, Int)] = keys.map(_ -> 5050)

  prepare(_.parallelize(input))
  verify(_ should containInAnyOrder(expected))

  /**
   * - `.groupByKey` shuffles everything over network before `.mapValues` can start
   * - `.reduceByKey` can reduce locally before shuffle
   *
   * - https://databricks.gitbooks.io/databricks-spark-knowledge-base/content/best_practices/prefer_reducebykey_over_groupbykey.html
   */
  baseline {
    _.groupByKey.mapValues(_.sum)
  }

  test("v1") {
    _.reduceByKey(???)
  }
}
