package scio.koans.a3_serde

import scio.koans.shared._

/**
 * Fix the `NotSerializableException`.
 */
class K04_WordCount1 extends PipelineKoan {
  ImNotDone

  import K04_WordCount1._

  val input: Seq[String] = Seq("a b c", "b c d", "c d e")
  val expected: Seq[(String, Long)] = Seq(("b", 2), ("c", 3), ("d", 2), ("e", 1))

  "Snippet" should "work" in {
    runWithContext { sc =>
      val tokenizer = new Tokenizer(Set("a"))

      val output = sc
        .parallelize(input)
        .flatMap(tokenizer.tokenize)
        .countByValue

      output should containInAnyOrder(expected)
    }
  }
}

object K04_WordCount1 {
  class Tokenizer(val stopWords: Set[String]) {
    def tokenize(line: String): Seq[String] = line
      .split("[^a-zA-Z']+")
      .filter { w =>
        w.nonEmpty && !stopWords.contains(w)
      }
  }
}
