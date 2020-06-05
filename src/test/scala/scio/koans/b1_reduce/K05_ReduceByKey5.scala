package scio.koans.b1_reduce

import com.spotify.scio.values.SCollection
import scio.koans.shared._

/**
 * Compute count, sum, min, max, and distinct count by key.
 */
class K05_ReduceByKey5 extends TransformKoan {
  ImNotDone

  import K05_ReduceByKey5._

  type InT = SCollection[(String, Int)]
  type OutT = SCollection[(String, Stats)]

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
  val expected: Seq[(String, Stats)] = Seq(
    ("a", Stats(6, 10, 1, 3, 3)),
    ("b", Stats(2, 9, 4, 5, 2)),
    ("c", Stats(1, 6, 6, 6, 1))
  )
  prepare(_.parallelize(input))
  verify(_ should containInAnyOrder(expected))

  baseline {
    _.groupByKey.mapValues(v => Stats(v.size, v.sum, v.min, v.max, v.toSet.size))
  }

  // How does this compare with `baseline` in terms of shuffle?
  test("v1") {
    _.mapValues(v => ?:[(Int, Int, Int, Int, Set[Int])])
      .reduceByKey(???)
      .mapValues(v => ?:[Stats])
  }
}

object K05_ReduceByKey5 {
  case class Stats(count: Int, sum: Int, min: Int, max: Int, distinctCount: Int)
}
