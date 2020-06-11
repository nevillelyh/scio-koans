package scio.koans.b4_approx

import com.spotify.scio.values.SCollection
import com.twitter.algebird._
import scio.koans.shared._

/**
 * Compute approximate distinct count with HyperLogLog.
 */
class K02_HyperLogLog extends TransformKoan {
  ImNotDone

  type InT = SCollection[String]
  type OutT = SCollection[Long]

  // 500 unique items
  val unique: Seq[String] = (1 to 500).map("item-%05d".format(_))

  // Fill up to 1000 items with duplicates
  val input: Seq[String] = unique ++ Seq.fill(500)(unique(scala.util.Random.nextInt(500)))
  val expected: Long = 500

  prepare {
    _.parallelize(input)
  }

  verify {
    _ should satisfySingleValue[Long] { c =>
      val error = (500L * 0.02).toLong
      c >= 500L - error && c < 500L + error
    }
  }

  baseline {
    _.distinct.count
  }

  test("v1") {
    _.countApproxDistinct(maximumEstimationError = 0.02)
  }

  test("v2") {
    // `HyperLogLogAggregator` is of type `Aggregator[Array[Byte], HLL, HLL]`
    val hll: Aggregator[String, HLL, Long] = HyperLogLogAggregator
      .withError(0.02)
      .composePrepare(???)
      .andThenPresent(???)
    _.aggregate(hll)
  }
}
