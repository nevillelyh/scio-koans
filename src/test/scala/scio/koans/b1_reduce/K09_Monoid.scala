package scio.koans.b1_reduce

import scio.koans.shared._

/**
 * Abstract reduce binary operations with `Monoid`s.
 */
class K09_Monoid extends Koan {
  ImNotDone

  import K09_Monoid._

  def testCombine[T](x: T, y: T, expected: T)(implicit mon: Monoid[T]): Unit =
    mon.combine(x, y) shouldBe expected

  def testCombineAll[T](xs: Seq[T], expected: T)(implicit mon: Monoid[T]): Unit =
    mon.combineAll(xs) shouldBe expected

  "Int monoid" should "work" in {
    // Tip: ⌘-⇧-P or Ctrl-Shift-P to show implicit arguments
    /** [[K09_Monoid.Monoid.intMonoid]] is applied implicitly. */
    testCombine(1, 2, 3)
    testCombineAll((1 to 10), 55)
  }

  "Min monoid" should "work" in {
    case class Min(v: Int)
    implicit val minMon: Monoid[Min] = Monoid(Min(Int.MaxValue))((x, y) => Min(math.min(x.v, y.v)))
    testCombine(Min(1), Min(2), Min(1))
    testCombineAll(Seq(Min(1), Min(2), Min(3)), Min(1))
  }

  "Max monoid" should "work" in {
    case class Max(v: Int)
    implicit val maxMon: Monoid[Max] = ???
    testCombine(Max(1), Max(2), Max(2))
    testCombineAll(Seq(Max(1), Max(2), Max(3)), Max(3))
  }

  "Set monoid" should "work" in {
    implicit val setMon: Monoid[Set[Int]] = Monoid(???)(???)
    testCombine(Set(1), Set(2), Set(1, 2))
    testCombineAll(Seq(Set(1), Set(2), Set(3)), Set(1, 2, 3))
  }
}

object K09_Monoid {
  trait Semigroup[T] {
    def combine(x: T, y: T): T
    def combineAllOption(xs: TraversableOnce[T]): Option[T] = xs.reduceOption(combine)
  }

  // A monoid is an algebraic structure with a single associative binary operation (`f`) and an
  // identity element (`z`).
  trait Monoid[T] extends Semigroup[T] {
    val zero: T
    def combineAll(xs: TraversableOnce[T]): T = combineAllOption(xs).getOrElse(zero)
  }

  object Monoid {
    // Make a `Monoid[T]` from type `T`, zero value `z`, and binary operation `f`
    def apply[T](z: T)(f: (T, T) => T): Monoid[T] = new Monoid[T] {
      override val zero: T = z
      override def combine(x: T, y: T): T = f(x, y)
    }

    // Implicit so it can be applied automatically to `(implicit mon: Monoid[Int])` arguments
    implicit val intMonoid: Monoid[Int] = Monoid(0)(_ + _)
  }
}
