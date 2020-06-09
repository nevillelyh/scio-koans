package scio.koans.b3_dofn

import java.util.UUID
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}

import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import scio.koans.shared._

/**
 * Understand `DoFn` parallelism and resource sharing.
 */
class K06_Parallelism2 extends PipelineKoan {
  ImNotDone

  import K06_Parallelism2._

  val input: Seq[Int] = 1 to 100
  val expected: Seq[Int] = 101 to 200

  "Snippet" should "work" in {
    val doFn = new MyDoFn

    runWithParallelism(8) { sc =>
      val p = sc.parallelize(input).applyTransform(ParDo.of(doFn))
      p should containInAnyOrder(expected)
    }

    // How many unique `id`s were used to run our `DoFn`?
    MyDoFn.sharedMap.size() shouldBe ?:[Int]

    // What's the single key-value pair in our map?
    // Would this work with `DataflowRunner`?
    MyDoFn.sharedMap.get(???) shouldBe ?:[Int]
  }
}

object K06_Parallelism2 {
  class MyDoFn extends DoFn[Int, Int] {
    // What happens to this after the `DoFn` is cloned for worker threads?
    val id: String = UUID.randomUUID().toString

    @ProcessElement def processElement(context: ProcessContext): Unit = {
      context.output(context.element() + 100)

      MyDoFn.sharedMap.putIfAbsent(id, 0)
      MyDoFn.sharedMap.compute(id, (k, v) => v + 1)
    }
  }

  object MyDoFn {
    val sharedMap: ConcurrentMap[String, Int] = new ConcurrentHashMap[String, Int]()
  }
}
