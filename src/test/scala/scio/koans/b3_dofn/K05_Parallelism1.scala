package scio.koans.b3_dofn

import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}

import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import scio.koans.shared._

import scala.collection.JavaConverters._

/**
 * Understand `DoFn` parallelism.
 */
class K05_Parallelism1 extends PipelineKoan {
  ImNotDone

  import K05_Parallelism1._

  val input: Seq[Int] = 1 to 100
  val expected: Seq[Int] = 101 to 200

  "Snippet" should "work" in {
    runWithParallelism(4) { sc =>
      val p = sc.parallelize(input).applyTransform(ParDo.of(new MyDoFn))
      p should containInAnyOrder(expected)
    }

    // How many unique threads were used to run our `DoFn`?
    MyDoFn.sharedMap.size() shouldBe ?:[Int]

    // What's the sum of all values in our map?
    MyDoFn.sharedMap.asScala.values.sum shouldBe ?:[Int]
  }
}

object K05_Parallelism1 {
  class MyDoFn extends DoFn[Int, Int] {
    @ProcessElement def processElement(context: ProcessContext): Unit = {
      context.output(context.element() + 100)

      val id = Thread.currentThread().getId
      MyDoFn.sharedMap.putIfAbsent(id, 0)
      MyDoFn.sharedMap.compute(id, (k, v) => v + 1)
    }
  }

  object MyDoFn {
    val sharedMap: ConcurrentMap[Long, Int] = new ConcurrentHashMap[Long, Int]()
  }
}
