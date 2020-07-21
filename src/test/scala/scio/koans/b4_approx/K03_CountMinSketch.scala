package scio.koans.b4_approx

import com.spotify.scio.values.SCollection
import com.twitter.algebird._
import scio.koans.shared._

/**
 * Compute approximate frequency with CountMinSketch.
 */
class K03_CountMinSketch extends TransformKoan {
  ImNotDone

  import K03_CountMinSketch._

  type InT = SCollection[String]
  type OutT = SCollection[(String, Long)]

  prepare {
    _.parallelize(input)
  }

  verify {
    // `eps` and `delta` for our `CMSMonoid` is low enough that we can test against exact result
    _ should containInAnyOrder(output)
  }

  baseline {
    _.countByValue.filter(x => x._1 == "a" || x._1 == "c" || x._1 == "e")
  }

  test("v1") { input =>
    val monoid = CMS.monoid[String](eps = 0.005, delta = 1e-8, seed = 0)
    val aggregator = CMSAggregator(monoid)
    input
      .aggregate(aggregator)
      .flatMap { cms =>
        Seq(
          ("a", cms.frequency("a").estimate),
          ???,
          ???
        )
      }
  }
}

object K03_CountMinSketch {
  val input: Seq[String] = Seq(
    Seq.fill(100)("a"),
    Seq.fill(80)("b"),
    Seq.fill(50)("c"),
    Seq.fill(20)("d"),
    Seq.fill(10)("e")
  ).flatten

  val output: Seq[(String, Long)] = Seq(("a", 100), ("c", 50), ("e", 10))
}
