package scio.koans.b3_dofn

import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

import org.apache.beam.sdk.transforms.DoFn.{FinishBundle, ProcessElement, Setup, StartBundle}
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.openjdk.jmh.annotations.TearDown
import scio.koans.shared._

/**
 * Understand `DoFn` life cycle.
 */
class K07_LifeCycle extends PipelineKoan {
  ImNotDone

  import K07_LifeCycle._

  val input: Seq[Int] = 1 to 100
  val expected: Seq[Int] = 101 to 200

  "Snippet" should "work" in {
    val doFn = new MyDoFn

    runWithParallelism(4) { sc =>
      val p = sc.parallelize(input).applyTransform(ParDo.of(doFn))
      p should containInAnyOrder(expected)
    }

    // How many times were `setup/startBundle/finishBundle/tearDown` called?
    MyDoFn.setups.get() shouldBe ?:[Int]

    // Number of bundles is non-deterministic, but there should be a minimal at least?
    MyDoFn.startBundles.get() should be >= ?:[Int]
    MyDoFn.finishBundles.get() should be >= ?:[Int]

    // Number of `startBundle` calls should equal to the number of what other method calls?
    MyDoFn.startBundles.get() shouldBe ?:[Int]

    // `DirectRunner` may not call `tearDown` at all
    // MyDoFn.tearDowns.get() shouldBe 4
  }
}

object K07_LifeCycle {
  class MyDoFn extends DoFn[Int, Int] {
    // What happens to this after the `DoFn` is cloned for worker threads?
    val id: String = UUID.randomUUID().toString

    @Setup def setup(): Unit =
      MyDoFn.setups.incrementAndGet()

    @StartBundle def startBundle(context: StartBundleContext): Unit = {
      val key = "StartBundle"
      MyDoFn.startBundles.incrementAndGet()
    }

    @ProcessElement def processElement(context: ProcessContext): Unit =
      context.output(context.element() + 100)

    @FinishBundle def finishBundle(context: FinishBundleContext): Unit =
      MyDoFn.finishBundles.incrementAndGet()

    @TearDown def tearDown(): Unit =
      MyDoFn.tearDowns.incrementAndGet()
  }

  object MyDoFn {
    val setups: AtomicInteger = new AtomicInteger(0)
    val startBundles: AtomicInteger = new AtomicInteger(0)
    val finishBundles: AtomicInteger = new AtomicInteger(0)
    val tearDowns: AtomicInteger = new AtomicInteger(0)
  }
}
