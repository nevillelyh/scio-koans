package scio.koans.b1_reduce

import com.spotify.scio.values.SCollection
import com.twitter.algebird._
import scio.koans.shared._

/**
 * Replace `foldByKey` with `combineByKey` or `aggregateByKey`.
 */
class K12_CombineAggregate extends TransformKoan {
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

  test("combineByKey") { input =>
    val min = input.combineByKey(identity)(math.min)(math.min)
    val max = ???

    def createCombiner(x: Int): Set[Int] = ???
    def mergeValue(c: Set[Int], x: Int): Set[Int] = ???
    def mergeCombiners(x: Set[Int], y: Set[Int]): Set[Int] = ???
    val distinctCount = input
      .combineByKey(createCombiner)(mergeValue)(mergeCombiners)
      .mapValues(_.size)

    (min, max, distinctCount)
  }

  test("aggregateByKey") { input =>
    val min = input.aggregateByKey(Int.MaxValue)(math.min, math.min)
    val max = ???

    def seqOp(accum: Set[Int], v: Int): Set[Int] = ???
    def combOp(x: Set[Int], y: Set[Int]): Set[Int] = ???
    val distinctCount = input
      .aggregateByKey(Set.empty[Int])(seqOp, combOp)
      .mapValues(_.size)

    (min, max, distinctCount)
  }
}
