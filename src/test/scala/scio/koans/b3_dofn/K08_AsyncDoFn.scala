package scio.koans.b3_dofn

import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

import com.spotify.scio.transforms.{DoFnWithResource, ScalaAsyncDoFn}
import org.apache.beam.sdk.transforms.ParDo
import scio.koans.shared._

import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future}

/**
 * Implement an `AsyncDoFn`.
 */
class K08_AsyncDoFn extends PipelineKoan {
  ImNotDone

  import K08_AsyncDoFn._

  val input: Seq[Int] = 1 to 100
  val expected: Seq[Int] = 101 to 200

  "Snippet" should "work" in {
    val doFn = new MyDoFn

    runWithParallelism(8) { sc =>
      val p = sc.parallelize(input).applyTransform(ParDo.of(doFn))
      p should containInAnyOrder(expected)
    }

    // We should reuse a single client instance
    Client.count.get() shouldBe 1
  }
}

object K08_AsyncDoFn {
  class MyDoFn extends ScalaAsyncDoFn[Int, Int, Client] {
    override def getResourceType: DoFnWithResource.ResourceType = ???

    override def createResource(): Client = ???

    override def processElement(input: Int): Future[Int] = ???
  }

  class Client {
    Client.count.incrementAndGet()

    implicit val es: ExecutionContextExecutorService =
      ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(8))

    // Process a request asynchronously
    def request(input: Int): Future[Int] = Future {
      Thread.sleep(100)
      input + 100
    }
  }

  object Client {
    val count: AtomicInteger = new AtomicInteger(0)
  }
}
