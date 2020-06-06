package scio.koans.a3_serde

import java.util.regex.Pattern

import com.google.common.base.Splitter
import scio.koans.shared._

import scala.collection.JavaConverters._

/**
 * Fix the `NotSerializableException`.
 */
class K06_WordCount3 extends PipelineKoan {
  ImNotDone

  import K06_WordCount3._

  val input: Seq[String] = Seq("a b c", "b c d", "c d e")
  val expected: Seq[(String, Long)] = Seq(("b", 2), ("c", 3), ("d", 2), ("e", 1))

  "Snippet" should "work" in {
    runWithContext { sc =>
      val tokenizer = new Tokenizer("[^a-zA-Z']+", Set("a"))

      val output = sc
        .parallelize(input)
        .flatMap(tokenizer.tokenize)
        .countByValue

      output should containInAnyOrder(expected)
    }
  }
}

object K06_WordCount3 {
  class Tokenizer(pattern: String, stopWords: Set[String]) extends Serializable {
    private val splitter = Splitter.on(Pattern.compile(pattern)).omitEmptyStrings()

    def tokenize(line: String): Iterable[String] =
      splitter.split(line).asScala.filter(!stopWords.contains(_))
  }
}
