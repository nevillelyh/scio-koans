package scio.koans.b1_reduce

import com.twitter.algebird._
import scio.koans.shared._

import scala.util.Random

/**
 * More aggregators.
 */
class K20_MoreAggregators3 extends Koan {
  ImNotDone

  def testAggregator[A, B, C](xs: Seq[A], expected: C)(aggregator: Aggregator[A, B, C]): Unit =
    aggregator(xs) shouldBe expected

  "Aggregator" should "support randomSample" in {
    // Fixed seed so that `sample.size` is deterministic
    val aggregator = Aggregator.randomSample[Int](0.1, seed = 1234)
    val sample = aggregator(1 to 100)
    sample.forall(x => x >= 1 && x <= 100) shouldBe ?:[Boolean]
    sample.size shouldBe 10 // This may vary depending on the seed
  }

  it should "support reservoirSample" in {
    val aggregator = Aggregator.reservoirSample[Int](10)
    val sample = aggregator(1 to 100)
    sample.forall(x => x >= 1 && x <= 100) shouldBe ?:[Boolean]
    sample.size shouldBe ?:[Int]
  }

  it should "support sorted{Reverse}Take" in {
    val xs = Random.shuffle(1 to 100)

    testAggregator(xs, Seq(1, 2, 3, 4, 5))(Aggregator.sortedTake(5))
    testAggregator(xs, Seq(100, 99, 98, 97, 96))(Aggregator.sortedReverseTake(5))
    testAggregator(xs, ???)(Aggregator.sortedTake(3))
    testAggregator(xs, Seq(100, 99, 98))(???)
  }

  it should "support sortBy{Reverse}Take" in {
    val xs = Random.shuffle((1 to 100).map(x => s"key-$x" -> x))

    val expected1: Seq[(String, Int)] =
      Seq("key-1" -> 1, "key-2" -> 2, "key-3" -> 3, "key-4" -> 4, "key-5" -> 5)
    testAggregator(xs, expected1)(Aggregator.sortByTake(5)(_._2))

    val expected2: Seq[(String, Int)] =
      Seq("key-100" -> 100, "key-99" -> 99, "key-98" -> 98, "key-97" -> 97, "key-96" -> 96)
    testAggregator(xs, expected2)(Aggregator.sortByReverseTake(5)(_._2))

    val expected3: Seq[(String, Int)] = ???
    testAggregator(xs, expected3)(Aggregator.sortByTake(3)(_._2))

    val expected4: Seq[(String, Int)] = Seq("key-100" -> 100, "key-99" -> 99, "key-98" -> 98)
    testAggregator(xs, expected4)(???)
  }
}
