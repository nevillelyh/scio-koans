package scio.koans.b1_reduce

import scio.koans.shared._

/**
 * Abstract reduce binary operations with `Aggregator`s.
 */
class K13_Aggregator extends Koan {
  ImNotDone

  import K13_Aggregator._

  def testAggregator[A, B, C](xs: Seq[A], expected: C)(aggregator: Aggregator[A, B, C]): Unit =
    aggregator(xs) shouldBe expected

  "Min aggregator" should "work" in {
    case class Min(v: Int)
    implicit val minSg: Semigroup[Min] = (x, y) => Min(math.min(x.v, y.v))
    val aggregator = Aggregator[Int, Min, Int](Min)(_.v)
    testAggregator(Seq(1, 2, 3), 1)(aggregator)
  }

  "Max aggregator" should "work" in {
    case class Max(v: Int)
    implicit val maxSg: Semigroup[Max] = (x, y) => Max(math.max(x.v, y.v))
    val aggregator = ???
    testAggregator(Seq(1, 2, 3), 1)(aggregator)
  }

  "Distinct count aggregator" should "work" in {
    implicit val setSg: Semigroup[Set[Int]] = Semigroup(_ ++ _)
    val aggregator = ???
    testAggregator(Seq(1, 1, 1, 2, 2, 3), 3)(aggregator)
  }
}

object K13_Aggregator {
  trait Semigroup[T] {
    def combine(x: T, y: T): T
    def combineAllOption(xs: TraversableOnce[T]): Option[T] = xs.reduceOption(combine)
  }

  object Semigroup {
    def apply[T](f: (T, T) => T): Semigroup[T] = (x: T, y: T) => f(x, y)
    implicit val intSemigroup: Semigroup[Int] = Semigroup(_ + _)
  }

  // `Aggregator` abstracts the `data.map(prepare).sum(semigroup.combine).map(present)` pattern
  trait Aggregator[A, B, C] {
    // Prepare input as intermediate summable type `B`
    def prepare(a: A): B

    // For summing up a collection of `B`s
    val semigroup: Semigroup[B]

    // Present sum as output type `C`
    def present(b: B): C

    def apply(xs: TraversableOnce[A]): C = present(xs.map(prepare).reduce(semigroup.combine))
  }

  object Aggregator {
    def apply[A, B, C](f: A => B)(g: B => C)(implicit sg: Semigroup[B]): Aggregator[A, B, C] =
      new Aggregator[A, B, C] {
        override def prepare(a: A): B = f(a)
        override val semigroup: Semigroup[B] = sg
        override def present(b: B): C = g(b)
      }
  }
}
