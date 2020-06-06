package scio.koans.a3_serde

import java.util.regex.Pattern

import com.google.common.base.Splitter
import scio.koans.shared._

import scala.collection.JavaConverters._

/**
 * Fix the `NotSerializableException`.
 */
class K05_WordCount2 extends PipelineKoan {
  ImNotDone

  val input: Seq[String] = Seq("a b c", "b c d", "c d e")
  val expected: Seq[(String, Long)] = Seq(("a", 1), ("b", 2), ("c", 3), ("d", 2), ("e", 1))

  "Snippet" should "work" in {
    runWithContext { sc =>
      // Hint: delay the `tokenizer` initialization
      val tokenizer = Splitter
        .on(Pattern.compile("[^a-zA-Z']+"))
        .omitEmptyStrings()

      val output = sc
        .parallelize(input)
        .flatMap(l => tokenizer.split(l).asScala)
        .countByValue

      output should containInAnyOrder(expected)
    }
  }
}
