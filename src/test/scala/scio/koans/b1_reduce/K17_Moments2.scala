package scio.koans.b1_reduce

import com.spotify.scio.values.SCollection
import com.twitter.algebird._
import scio.koans.shared._

/**
 * Compute min, max, sum, count, mean, variance and standard deviation with Moments.
 */
class K17_Moments2 extends TransformKoan {
  ImNotDone

  import K17_Moments2._

  type InT = SCollection[Int]
  type OutT = SCollection[Stats]

  prepare(_.parallelize(input))
  verify(_ should satisfySingleValue[Stats] { actual =>
    // Handle floating point error
    def approxEq(x: Double, y: Double): Boolean =
      if (x == 0) {
        math.abs(y) < 1e-10
      } else if (y == 0) {
        math.abs(x) < 1e-10
      } else {
        math.abs(x - y) / math.abs(y) < 1e-10
      }

    actual.min == expected.min &&
    actual.max == expected.max &&
    actual.sum == expected.sum &&
    actual.count == expected.count &&
    approxEq(actual.mean, expected.mean) &&
    approxEq(actual.variance, expected.variance) &&
    approxEq(actual.stddev, expected.stddev)
  })

  baseline { input =>
    val min = input.min
    val max = input.max

    val sumCountMean = input
      .map(x => (x, 1L))
      .sum
      .map {
        case (sum, count) =>
          (sum, count, sum.toDouble / count)
      }
    val varianceAndStddev = input
      .cross(sumCountMean.map(_._3))
      .map { case (x, mean) => (math.pow(x - mean, 2), 1L) }
      .sum
      .map {
        case (sqSum, count) =>
          val variance = sqSum / count
          val stddev = math.sqrt(variance)
          (variance, stddev)
      }

    // Each cross creates a nested tuple, a little cryptic
    min.cross(max).cross(sumCountMean).cross(varianceAndStddev).map {
      case (((min, max), (sum, count, mean)), (variance, stddev)) =>
        Stats(min, max, sum, count, mean, variance, stddev)
    }
  }

  test("v1") {
    _.map(x => (Min(x), Max(x), x, Moments(x))).sum
      .map {
        case (min, max, sum, m) =>
          ?:[Stats]
      }
  }

  test("v2") { input =>
    val min = Aggregator.min[Int]
    val max = Aggregator.max[Int]
    val sum = Aggregator.fromMonoid[Int]
    val moments: Aggregator[Int, Moments, Moments] = ???

    // Compose from multiple aggregators
    val multiAggregator: Aggregator[Int, _, Stats] = ???

    input.aggregate(multiAggregator)
  }
}

object K17_Moments2 {
  val input: Seq[Int] = 1 to 100

  val expected: Stats = {
    val min = input.min
    val max = input.max
    val sum = input.sum
    val count = input.size
    val mean = input.sum.toDouble / count
    val variance = input.map(_ - mean).map(math.pow(_, 2)).sum / count
    val stddev = math.sqrt(variance)
    Stats(min, max, sum, count, mean, variance, stddev)
  }

  case class Stats(
    min: Int,
    max: Int,
    sum: Int,
    count: Long,
    mean: Double,
    variance: Double,
    stddev: Double
  )
}
