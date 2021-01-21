package scio.koans.a1_collections

import org.openjdk.jmh.annotations._
import scio.koans.shared._

import scala.collection.JavaConverters._

/**
 * Convert a Scala `Seq[Int]` to Java `List[Integer]`.
 */
class K11_Erasure extends JmhKoan {
  ImNotDone

  val numbers: Seq[Int] = 1 to 100

  def sum(xs: java.util.List[java.lang.Integer]): java.lang.Integer = {
    var r = 0
    var i = 0
    while (i < xs.size()) {
      r += xs.get(i)
      i += 1
    }
    r
  }

  @Benchmark def baseline: Int = sum(numbers.map(_.asInstanceOf[java.lang.Integer]).asJava)

  // Hint: `scala.Int` and `java.lang.Integer` are the same boxed integer when inside a generic
  // collection. Therefore `Seq[Int]` and `Seq[java.lang.Integer]` are binary compatible despite
  // the type mismatch.
  @Benchmark def v1: Int = ???

  verifyResults()
  verifySpeedup(Speedup.Times(3))
}
