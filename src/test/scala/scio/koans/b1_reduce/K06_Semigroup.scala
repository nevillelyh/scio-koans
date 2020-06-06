package scio.koans.b1_reduce

import scio.koans.shared._

/**
 * Abstract reduce binary operations with `Semigroup`s.
 */
class K06_Semigroup extends Koan {
  ImNotDone

  import K06_Semigroup._

  // `Semigroup[T]` argument is usually implicit.
  // https://www.lyh.me/implicits.html
  def testCombine[T](x: T, y: T, expected: T)(implicit sg: Semigroup[T]): Unit =
    sg.combine(x, y) shouldBe expected

  def testCombineAllOption[T](xs: Seq[T], expected: Option[T])(implicit sg: Semigroup[T]): Unit =
    sg.combineAllOption(xs) shouldBe expected

  "Int semigroup" should "work" in {
    // Tip: ⌘-⇧-P or Ctrl-Shift-P to show implicit arguments
    /** [[K06_Semigroup.Semigroup.intSemigroup]] is applied implicitly. */
    testCombine(1, 2, 3)
    testCombineAllOption((1 to 10), Some(55))
  }

  "Min semigroup" should "work" in {
    case class Min(v: Int)
    implicit val minSemigroup: Semigroup[Min] = (x, y) => Min(math.min(x.v, y.v))
    testCombine(Min(1), Min(2), Min(1))
    testCombineAllOption(Seq(Min(1), Min(2), Min(3)), Some(Min(1)))
  }

  "Max semigroup" should "work" in {
    case class Max(v: Int)
    // Hint: max is the opposite of min
    implicit val maxSemigroup: Semigroup[Max] = ???
    testCombine(Max(1), Max(2), Max(2))
    testCombineAllOption(Seq(Max(1), Max(2), Max(3)), Some(Max(3)))
  }

  "Set semigroup" should "work" in {
    implicit val setSemigroup: Semigroup[Set[Int]] = new Semigroup[Set[Int]] {
      override def combine(x: Set[Int], y: Set[Int]): Set[Int] = ???

      override def combineAllOption(xs: TraversableOnce[Set[Int]]): Option[Set[Int]] =
        if (xs.isEmpty) {
          None
        } else {
          // `xs.reduceOption(combine)` makes many temporary `Set[Int]`s
          // Use a mutable builder to reduce overhead
          val s = Set.newBuilder[Int]
          xs.foreach { x =>
            ???
          }
          Some(s.result())
        }
    }
    testCombine(Set(1), Set(2), Set(1, 2))
    testCombineAllOption(Seq(Set(1), Set(2), Set(3)), Some(Set(1, 2, 3)))
  }
}

object K06_Semigroup {
  // A semigroup is an algebraic structure consisting of a set (`T`) together with an associative
  // binary operation (`combine`).
  // https://www.lyh.me/slides/semigroups.html
  trait Semigroup[T] {
    def combine(x: T, y: T): T
    def combineAllOption(xs: TraversableOnce[T]): Option[T] = xs.reduceOption(combine)
  }

  object Semigroup {
    // Make a `Semigroup[T]` from type `T` and binary operation `f`
    def apply[T](f: (T, T) => T): Semigroup[T] = (x: T, y: T) => f(x, y)

    // Companion object of `Semigroup[T]` is searched for implicit `Semigroup[Int]`
    // https://docs.scala-lang.org/tutorials/FAQ/finding-implicits.html
    // Implicit so it can be applied automatically to `(implicit sg: Semigroup[Int])` arguments
    implicit val intSemigroup: Semigroup[Int] = Semigroup(_ + _)
  }
}
