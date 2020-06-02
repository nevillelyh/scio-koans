package scio.koans.a1_collections

import java.{util => ju}
import scio.koans.shared._
import org.openjdk.jmh.annotations._

import scala.collection.JavaConverters._

/**
 * Convert a Scala `List[String]` to a Java `List[CharSequence]`.
 */
class K01_Converter1 extends JmhKoan {
  ImNotDone

  private val uuids: List[String] = List.fill(1000)(ju.UUID.randomUUID().toString)

  /**
   * - `.asJava` lazily wraps a collection
   * - `.map` eagerly copies a collection, O(n)
   * - `String` is a sub-type of `CharSequence`
   * - `ju.List[String]` is not a sub-type of `ju.List[CharSequence]` because `ju.List` is invariant
   * - `.asInstanceOf[T]`, i.e. casting, has low runtime overhead
   *
   * - https://docs.scala-lang.org/tour/variances.html
   * - http://www.lyh.me/how-many-copies.html
   */
  @Benchmark def baseline: ju.List[CharSequence] = uuids.map(_.asInstanceOf[CharSequence]).asJava

  // Hint: casting can be parameterized, i.e. `.asInstanceOf[M[T]]`
  @Benchmark def v1: ju.List[CharSequence] = ???

  verifyResults()
  verifySpeedup(Speedup.Times(500))
}
