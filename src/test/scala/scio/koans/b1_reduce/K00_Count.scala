package scio.koans.b1_reduce

import com.spotify.scio.values.SCollection
import scio.koans.shared._

/**
 * A `TransformKoan` tests Scio transforms, i.e. `SCollection[InT] => SCollection[OutT]`.
 */
class K00_Count extends TransformKoan {
  ImNotDone // FIXME: delete this to move on to the next one

  // Input type
  type InT = SCollection[String]

  // Output type
  type OutT = SCollection[Long]

  val input = Seq("a", "b", "c", "d", "e")
  val expected = 5L

  // Prepare input before each test
  prepare(_.parallelize(input))

  // Verify output after each test
  verify(_ should containSingleValue(expected))

  baseline {
    _.count
  }

  // FIXME: implement this to make the test pass
  test("v1") {
    // Hint: `map` elements to `Long`
    _.map(_ => ?:[Long]).reduce(???)
  }

  // Add as many alternatives as you like
  // test("v2") { ??? }
}
