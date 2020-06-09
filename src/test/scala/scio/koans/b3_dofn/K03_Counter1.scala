package scio.koans.b3_dofn

import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import scio.koans.shared._

/**
 * Understand `DoFn` serialization.
 */
class K03_Counter1 extends PipelineKoan {
  ImNotDone

  import K03_Counter1._

  val input: Seq[Int] = 1 to 10
  val expected: Seq[Int] = 101 to 110

  "Snippet" should "work" in {
    val doFn = new MyDoFn

    // Set `--targetParallelism=8`
    runWithParallelism(8) { sc =>
      // Is the same `doFn` instance being executed in all worker threads?
      val p = sc.parallelize(input).applyTransform(ParDo.of(doFn))
      p should containInAnyOrder(expected)
    }

    // Is this `doFn` instance executed by any worker at all?
    doFn.counter shouldBe ?:[Int]
  }
}

object K03_Counter1 {
  class MyDoFn extends DoFn[Int, Int] {
    var counter = 0

    @ProcessElement def processElement(context: ProcessContext): Unit = {
      context.output(context.element() + 100)
      counter += 1
    }
  }
}
