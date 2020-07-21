package scio.koans.b1_reduce

import com.twitter.algebird._
import scio.koans.shared._

/**
 * More aggregators.
 */
class K19_MoreAggregators1 extends Koan {
  ImNotDone

  def testAggregator[A, B, C](xs: Seq[A], expected: C)(aggregator: Aggregator[A, B, C]): Unit =
    aggregator(xs) shouldBe expected

  "Aggregator" should "support size" in {
    testAggregator(1 to 100, 100L)(Aggregator.size)
    testAggregator(1 to 50, ???)(Aggregator.size)
  }

  it should "support count" in {
    testAggregator(1 to 100, 50L)(Aggregator.count(_ % 2 == 0))
    testAggregator(1 to 100, ???)(Aggregator.count(_ % 2 == 1))
  }

  it should "support uniqueCount" in {
    testAggregator(Seq("a", "a", "a", "b", "b", "c"), 3)(Aggregator.uniqueCount)
    testAggregator(Seq("a", "a", "b"), ???)(Aggregator.uniqueCount)
  }

  it should "support numericSum" in {
    testAggregator(1 to 100, 5050.0)(Aggregator.numericSum)
    testAggregator(1 to 10, ???)(Aggregator.numericSum)
  }

  it should "support min/max" in {
    testAggregator((1 to 100), 1)(Aggregator.min)
    testAggregator((1 to 100), 100)(Aggregator.max)
    testAggregator(Seq("a", "b", "c"), ???)(Aggregator.min)
    testAggregator(Seq("a", "b", "c"), "c")(???)
  }
}
