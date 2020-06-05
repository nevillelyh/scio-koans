package scio.koans.b1_reduce

import com.spotify.scio.values.SCollection
import scio.koans.shared._

/**
 * Count distinct values by key.
 */
class K04_ReduceByKey4 extends TransformKoan {
  ImNotDone

  type InT = SCollection[(String, Int)]
  type OutT = SCollection[(String, Long)]

  val input: Seq[(String, Int)] = Seq(
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
  val expected: Seq[(String, Long)] = Seq(("a", 3L), ("b", 2L), ("c", 1L))

  prepare(_.parallelize(input))
  verify(_ should containInAnyOrder(expected))

  baseline {
    _.groupByKey.mapValues(_.toSet.size)
  }

  // How does this compare with `baseline` in terms of shuffle?
  test("v1") {
    _.mapValues(v => ?:[Set[Int]])
      .reduceByKey(???)
      .mapValues(???)
  }
}
