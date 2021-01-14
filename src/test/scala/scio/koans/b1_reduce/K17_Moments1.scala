package scio.koans.b1_reduce

import com.spotify.scio.values.SCollection
import com.twitter.algebird._
import scio.koans.shared._

/**
 * Compute count, mean, variance and standard deviation with Moments.
 */
class K17_Moments1 extends TransformKoan {
  ImNotDone

  import K17_Moments1._

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

    actual.count == expected.count &&
    approxEq(actual.mean, expected.mean) &&
    approxEq(actual.variance, expected.variance) &&
    approxEq(actual.stddev, expected.stddev)
  })

  baseline { input =>
    val countAndMean = input
      .map(x => (1L, x))
      .sum
      .map { case (count, sum) =>
        (count, sum.toDouble / count)
      }
    val varianceAndStddev = input
      .cross(countAndMean.values)
      .map { case (x, mean) => (math.pow(x - mean, 2), 1L) }
      .sum
      .map { case (sqSum, count) =>
        val variance = sqSum / count
        val stddev = math.sqrt(variance)
        (variance, stddev)
      }
    countAndMean.cross(varianceAndStddev).map { case ((count, mean), (variance, stddev)) =>
      Stats(count, mean, variance, stddev)
    }
  }

  test("v1") {
    // `Moments` can compute mean, count, variance, standard deviation, etc. in parallel.
    _.map(x => Moments(x)).sum // Algebird provides implicit `Semigroup[Moments]`
      .map(m => ?:[Stats])
  }

  test("v2") { input =>
    // `MomentsAggregator` is of type `MonoidAggregator[Double, Moments, Moments]`
    // We want `Int => Double` in `prepare` and `Moments` => `Stats` in `present`
    val aggregator: Aggregator[Int, Moments, Stats] = MomentsAggregator
      .composePrepare((x: Int) => ???)
      .andThenPresent(m => ???)
    input.aggregate(aggregator)
  }
}

object K17_Moments1 {
  val input: Seq[Int] = 1 to 100

  val expected: Stats = {
    val count = input.size
    val mean = input.sum.toDouble / count
    val variance = input.map(_ - mean).map(math.pow(_, 2)).sum / count
    val stddev = math.sqrt(variance)
    Stats(count, mean, variance, stddev)
  }

  case class Stats(count: Long, mean: Double, variance: Double, stddev: Double)
}
