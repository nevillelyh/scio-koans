package scio.koans.b1_reduce

import com.spotify.scio.values.SCollection
import com.twitter.algebird._
import scio.koans.shared._

/**
 * Replace `foldByKey` with `aggregateByKey`.
 */
class K14_AggregateByKey1 extends TransformKoan {
  ImNotDone

  type InT = SCollection[(String, Int)]
  type OutT = (SCollection[(String, Int)], SCollection[(String, Int)], SCollection[(String, Int)])

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
  val expectedMin: Seq[(String, Int)] = Seq(("a", 1), ("b", 4), ("c", 6))
  val expectedMax: Seq[(String, Int)] = Seq(("a", 3), ("b", 5), ("c", 6))
  val expectedDistinctCount: Seq[(String, Int)] = Seq(("a", 3), ("b", 2), ("c", 1))

  prepare(_.parallelize(input))
  verify {
    case (min, max, distinctCount) =>
      min should containInAnyOrder(expectedMin)
      max should containInAnyOrder(expectedMax)
      distinctCount should containInAnyOrder(expectedDistinctCount)
  }

  baseline { input =>
    val min = input.mapValues(Min(_)).foldByKey.mapValues(_.get)
    val max = input.mapValues(Max(_)).foldByKey.mapValues(_.get)
    val distinctCount = input.mapValues(Set(_)).foldByKey.mapValues(_.size)

    (min, max, distinctCount)
  }

  test("v1") { input =>
    // `MinAggregator` from Algebird
    val min: SCollection[(String, Int)] = input.aggregateByKey(MinAggregator())

    // FIXME: implement these with `aggregateByKey`
    // Hint: Algebird also provides `MaxAggregator`.
    val max: SCollection[(String, Int)] = ???

    val distinctCountAggregator: Aggregator[Int, Set[Int], Int] = Aggregator.uniqueCount[Int]
    val distinctCount: SCollection[(String, Int)] = ???

    (min, max, distinctCount)
  }
}
