package scio.koans.b1_reduce

import com.spotify.scio.values.SCollection
import com.twitter.algebird._
import scio.koans.shared._

/**
 * Replace `reduceByKey` with `foldByKey`.
 */
class K11_FoldByKey2 extends TransformKoan {
  ImNotDone

  import K11_FoldByKey2._

  type InT = SCollection[(String, Int)]
  type OutT = SCollection[(String, Stats)]

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
  val expected: Seq[(String, Stats)] = Seq(
    ("a", Stats(6, 10, 1, 3, 3)),
    ("b", Stats(2, 9, 4, 5, 2)),
    ("c", Stats(1, 6, 6, 6, 1))
  )
  prepare(_.parallelize(input))
  verify(_ should containInAnyOrder(expected))

  baseline {
    _.mapValues(v => (1, v, v, v, Set(v)))
      .reduceByKey { (x, y) =>
        (x._1 + y._1, x._2 + y._2, math.min(x._3, y._3), math.max(x._4, y._4), x._5 ++ y._5)
      }
      .mapValues(v => Stats(v._1, v._2, v._3, v._4, v._5.size))
  }

  // Hint: Algebird provides implicit `Monoid[T]` for `T = Int, Min[Int], Max[Int], Set[Int]`
  // And can derive one for tuples e.g. `Monoid[(Int, Int, Min[Int], Max[Int], Set[Int])]`
  test("v1") {
    _.mapValues(v => ?:[(Int, Int, Min[Int], Max[Int], Set[Int])]).foldByKey
      .mapValues(v => ?:[Stats])
  }
}

object K11_FoldByKey2 {
  case class Stats(count: Int, sum: Int, min: Int, max: Int, distinctCount: Int)
}
