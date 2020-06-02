package scio.koans.a1_collections

import java.{util => ju}
import scio.koans.shared._
import org.openjdk.jmh.annotations._

import scala.collection.JavaConverters._

/**
 * Convert a Java `List[CharSequence]` to a Scala `List[String]`.
 */
class K02_Converter2 extends JmhKoan {
  ImNotDone

  private val uuids: ju.List[CharSequence] = {
    val l = new ju.ArrayList[CharSequence]()
    (1 to 1000).foreach(_ => l.add(ju.UUID.randomUUID().toString))
    l
  }

  /**
   * - `.asScala` lazily wraps a collection
   * - `.map` and `.toList` eagerly copies a collection, O(n)
   * - `.iterator` is lazy
   */
  @Benchmark def baseline: List[String] = uuids.asScala.map(_.toString).toList

  // Hint: reduce eager copies by removing one conversion
  @Benchmark def v1: Seq[String] = ???

  // Hint: reduce eager copies by using `.iterator`
  @Benchmark def v2: List[String] = ???

  verifyResults()
  verifySpeedup(Speedup.Faster)
}
