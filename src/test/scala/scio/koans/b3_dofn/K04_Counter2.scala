package scio.koans.b3_dofn

import java.util.concurrent.atomic.AtomicInteger

import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import scio.koans.shared._

/**
 * Understand `DoFn` serialization and resource sharing.
 */
class K04_Counter2 extends PipelineKoan {
  ImNotDone

  import K04_Counter2._

  val input: Seq[Int] = 1 to 10
  val expected: Seq[Int] = 101 to 110

  "Snippet" should "work" in {
    val doFn = new MyDoFn

    runWithParallelism(8) { sc =>
      val p = sc.parallelize(input).applyTransform(ParDo.of(doFn))
      p should containInAnyOrder(expected)
    }

    doFn.counter shouldBe ?:[Int]

    // Would this work with `DataflowRunner`?
    MyDoFn.sharedCounter.get() shouldBe ?:[Int]
  }
}

object K04_Counter2 {
  class MyDoFn extends DoFn[Int, Int] {
    var counter = 0

    @ProcessElement def processElement(context: ProcessContext): Unit = {
      context.output(context.element() + 100)
      counter += 1
      MyDoFn.sharedCounter.incrementAndGet()
    }
  }

  object MyDoFn {
    // What's the different between this and `counter` in `MyDoFn`?
    // Why is it not a plain `Int`?
    val sharedCounter = new AtomicInteger(0)
  }
}
