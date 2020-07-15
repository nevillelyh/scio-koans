package scio.koans.b1_reduce

import com.spotify.scio.values.SCollection
import com.twitter.algebird._
import scio.koans.shared._

/**
 * Replace `foldByKey` with `aggregateByKey`.
 */
class K15_AggregateByKey2 extends TransformKoan {
  ImNotDone

  import K15_AggregateByKey2._

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
    ("a", Stats(6L, 10, 1, 3, 3)),
    ("b", Stats(2L, 9, 4, 5, 2)),
    ("c", Stats(1L, 6, 6, 6, 1))
  )
  prepare(_.parallelize(input))
  verify(_ should containInAnyOrder(expected))

  baseline {
    _.mapValues(v => (1L, v, Min(v), Max(v), Set(v))).foldByKey
      .mapValues(v => Stats(v._1, v._2, v._3.get, v._4.get, v._5.size))
  }

  // Individual aggregators
  val countA = Aggregator.size
  val sumA = Aggregator.fromMonoid[Int]
  val minA: Aggregator[Int, _, Int] = ???
  val maxA: Aggregator[Int, _, Int] = ???
  val distinctCountA: Aggregator[Int, _, Int] = ???

  test("v1") { input =>
    // Joining aggregators
    val multiAggregator: Aggregator[Int, _, Stats] =
      countA
        .join(sumA)
        .join(minA)
        .join(maxA)
        .join(distinctCountA)
        .andThenPresent {
          case ((((count, sum), min), max), distinctCount) =>
            // FIXME: present results as `Stats`
            ???
        }
    input.aggregateByKey(multiAggregator)
  }

  test("v2") { input =>
    // Compose from multiple aggregators
    val multiAggregator: Aggregator[Int, _, Stats] =
      MultiAggregator(countA, sumA, minA, maxA, distinctCountA)
        .andThenPresent { c =>
          // FIXME: present `c` as `Stats`
          ???
        }
    input.aggregateByKey(multiAggregator)
  }
}

object K15_AggregateByKey2 {
  case class Stats(count: Long, sum: Int, min: Int, max: Int, distinctCount: Int)
}
