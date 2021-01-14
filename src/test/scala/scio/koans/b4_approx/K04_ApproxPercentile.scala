package scio.koans.b4_approx

import com.spotify.scio.values.SCollection
import com.twitter.algebird._
import scio.koans.shared._

/**
 * Compute approximate 25, 50 (median), and 75 percentile.
 */
class K04_ApproxPercentile extends TransformKoan {
  ImNotDone

  import K04_ApproxPercentile._

  type InT = SCollection[Int]
  type OutT = SCollection[(Bound, Bound, Bound)]

  prepare {
    _.parallelize(0 to 100)
  }

  verify {
    _ should satisfySingleValue[(Bound, Bound, Bound)] { case (p25, p50, p75) =>
      p25.contains(25) && p50.contains(50) && p75.contains(75)
    }
  }

  baseline {
    _.groupBy(_ => ()).values
      .map { xs =>
        val sorted = xs.toList.sorted
        val i25 = (sorted.size / 4.0).toInt
        val i50 = (sorted.size / 2.0).toInt
        val i75 = (sorted.size / 4.0 * 3.0).toInt
        val p25 = Bound(sorted(i25), sorted(i25 + 1))
        val p50 = Bound(sorted(i50), sorted(i50 + 1))
        val p75 = Bound(sorted(i75), sorted(i75 + 1))
        (p25, p50, p75)
      }
  }

  test("v1") { input =>
    val p25: Aggregator[Int, _, Bound] =
      Aggregator
        .approximatePercentileBounds[Int](0.25)
        .andThenPresent(b => Bound(b.lower.lower, b.upper.upper))
    val p50: Aggregator[Int, _, Bound] = ???
    val p75: Aggregator[Int, _, Bound] = ???
    input.aggregate(MultiAggregator(p25, p50, p75))
  }
}

object K04_ApproxPercentile {
  case class Bound(lower: Double, upper: Double) {
    def contains(x: Int): Boolean = lower <= x && x <= upper
  }
}
