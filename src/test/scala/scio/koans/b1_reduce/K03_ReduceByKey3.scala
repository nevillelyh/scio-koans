package scio.koans.b1_reduce

import com.spotify.scio.values.SCollection
import scio.koans.shared._

/**
 * Compute min and max values by key.
 */
class K03_ReduceByKey3 extends TransformKoan {
  ImNotDone

  type InT = SCollection[(String, Int)]
  type OutT = SCollection[(String, (Int, Int))]

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
  val expected: Seq[(String, (Int, Int))] = Seq(("a", (1, 3)), ("b", (4, 5)), ("c", (6, 6)))

  prepare(_.parallelize(input))
  verify(_ should containInAnyOrder(expected))

  baseline {
    _.groupByKey.mapValues(v => (v.min, v.max))
  }

  // Hint: `Seq(10).min = 10`, `Seq(10).max = 10`
  // How does this compare with `baseline` in terms of shuffle?
  test("v1") {
    _.mapValues(v => ?:[(Int, Int)])
      .reduceByKey(???)
  }
}
