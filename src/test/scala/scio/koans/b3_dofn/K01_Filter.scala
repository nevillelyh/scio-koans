package scio.koans.b3_dofn

import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import scio.koans.shared._

/**
 * Implement `filter` with `DoFn`.
 */
class K01_Filter extends TransformKoan {
  ImNotDone

  import K01_Filter._

  type InT = SCollection[Int]
  type OutT = SCollection[Int]

  val input: Seq[Int] = 1 to 10
  val expected: Seq[Int] = Seq(2, 4, 6, 8, 10)

  prepare(_.parallelize(input))
  verify(_ should containInAnyOrder(expected))

  baseline {
    _.filter(_ % 2 == 0)
  }

  test("v1") {
    _.applyTransform(ParDo.of(new MyDoFn))
  }
}

object K01_Filter {
  class MyDoFn extends DoFn[Int, Int] {
    @ProcessElement def processElement(context: ProcessContext): Unit = ???
  }
}
