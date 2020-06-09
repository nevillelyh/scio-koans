package scio.koans.b3_dofn

import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import scio.koans.shared._

/**
 * Implement `flatMap` with `DoFn`.
 */
class K02_FlatMap extends TransformKoan {
  ImNotDone

  import K02_FlatMap._

  type InT = SCollection[Int]
  type OutT = SCollection[Int]

  val input: Seq[Int] = 1 to 5
  val expected: Seq[Int] = Seq(1, 2, 2, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 5)

  prepare(_.parallelize(input))
  verify(_ should containInAnyOrder(expected))

  baseline {
    _.flatMap(x => Iterable.fill(x)(x))
  }

  test("v1") {
    _.applyTransform(ParDo.of(new MyDoFn))
  }
}

object K02_FlatMap {
  class MyDoFn extends DoFn[Int, Int] {
    @ProcessElement def processElement(context: ProcessContext): Unit = ???
  }
}
