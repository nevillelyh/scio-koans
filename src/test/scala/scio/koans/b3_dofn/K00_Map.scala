package scio.koans.b3_dofn

import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import scio.koans.shared._

/**
 * Implement `map` with `DoFn`.
 */
class K00_Map extends TransformKoan {
  ImNotDone

  import K00_Map._

  type InT = SCollection[Int]
  type OutT = SCollection[Int]

  val input: Seq[Int] = 1 to 10
  val expected: Seq[Int] = 101 to 110

  prepare(_.parallelize(input))
  verify(_ should containInAnyOrder(expected))

  baseline {
    _.map(_ + 100)
  }

  test("v1") {
    _.applyTransform(ParDo.of(new MyDoFn))
  }
}

object K00_Map {
  // https://beam.apache.org/documentation/programming-guide/#pardo
  class MyDoFn extends DoFn[Int, Int] {
    @ProcessElement def processElement(context: ProcessContext): Unit = ???
  }
}
