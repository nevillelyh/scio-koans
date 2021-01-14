package scio.koans.b4_approx

import com.spotify.scio.values.SCollection
import com.spotify.scio.hash._
import magnolify.guava.auto._
import scio.koans.shared._

/**
 * Subtract a small collection from a larger one.
 */
class K01_BloomFilter2 extends TransformKoan {
  ImNotDone

  type InT = (SCollection[String], SCollection[String])
  type OutT = SCollection[String]

  import K01_BloomFilter2._

  prepare { sc =>
    (sc.parallelize(includes), sc.parallelize(excludes))
  }
  verify {
    _ should satisfy[String] { output =>
      // Output might be smaller than expected due to over filtering of false positives
      // But the number of missing ones should be no more than 3% of 1000
      output.toSet.subsetOf(expected) &&
      (expected.size - output.toSet.size).toDouble / 1000 <= 0.03
    }
  }

  baseline { case (includes, excludes) =>
    includes
      .keyBy(identity)
      .leftOuterJoin(excludes.keyBy(identity))
      .filter(_._2._2.isEmpty)
      .keys
  }

  test("v1") { case (includes, excludes) =>
    val bf = excludes.asApproxFilter(BloomFilter, 110, 0.03)
    includes
      .cross(bf)
      .filter {
        // Filter out items in RHS, i.e. `excludes`
        ???
      }
      .keys
  }
}

object K01_BloomFilter2 {
  val includes: Seq[String] = (1 to 1000).map("item-%05d".format(_))
  val excludes: Seq[String] = (1 to 100).map("item-%05d".format(_))
  val expected: Set[String] = includes.toSet -- excludes.toSet
}
