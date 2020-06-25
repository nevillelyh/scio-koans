package scio.koans.b1_reduce

import com.twitter.algebird._
import scio.koans.shared._

/**
 * More aggregators.
 */
class K19_MoreAggregators2 extends Koan {
  ImNotDone

  def testAggregator[A, B, C](xs: Seq[A], expected: C)(aggregator: Aggregator[A, B, C]): Unit =
    aggregator(xs) shouldBe expected

  "Aggregator" should "support minBy/maxBy" in {
    val xs = Seq(
      ("a", 2),
      ("b", 3),
      ("c", 1)
    )
    testAggregator(xs, ("a", 2))(Aggregator.minBy(_._1))
    testAggregator(xs, ("c", 1))(???)
    testAggregator(xs, ???)(Aggregator.minBy(_._2))
    testAggregator(xs, ("b", 3))(???)
  }

  it should "support exists" in {
    testAggregator(1 to 100, ???)(Aggregator.exists(_ % 2 == 0))
    testAggregator(1 to 100, ???)(Aggregator.exists(_ % 2 == 1))
  }

  it should "support forall" in {
    testAggregator(0 to 100 by 2, true)(Aggregator.forall(_ % 2 == 0))
    testAggregator(1 to 99 by 2, ???)(Aggregator.forall(_ % 2 == 1))
  }
}
