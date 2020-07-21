package scio.koans.b3_dofn

import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

import com.google.common.cache.{Cache, CacheBuilder}
import com.spotify.scio.transforms.BaseAsyncLookupDoFn.CacheSupplier
import com.spotify.scio.transforms.ScalaAsyncLookupDoFn
import org.apache.beam.sdk.transforms.ParDo
import scio.koans.shared._

import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future}

/**
 * Implement an `AsyncLookupDoFn`.
 */
class K09_AsyncLookupDoFn extends PipelineKoan {
  ImNotDone

  import K09_AsyncLookupDoFn._

  val input: Seq[Int] = Seq(1, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 4, 4, 5, 6)
  val expected: Seq[(Int, String)] = Seq(
    (1, "one"),
    (1, "one"),
    (1, "one"),
    (1, "one"),
    (1, "one"),
    (2, "two"),
    (2, "two"),
    (2, "two"),
    (2, "two"),
    (3, "three"),
    (3, "three"),
    (3, "three"),
    (4, "four"),
    (4, "four"),
    (5, "five"),
    (6, "unknown")
  )

  "Snippet" should "work" in {
    val doFn = new MyDoFn

    runWithParallelism(8) { sc =>
      val p = sc
        .parallelize(input)
        .applyTransform(ParDo.of(doFn))
        .map { kv =>
          // Handle failure and convert lookup result to `(Int, String)`
          ?:[(Int, String)]
        }
      p should containInAnyOrder(expected)
    }

    // Hint: there are 16 input elements with some duplicates
    Client.requestCount.get() should be <= ?:[Int]
  }
}

object K09_AsyncLookupDoFn {
  // Cache lookup results
  val cacheSupplier: CacheSupplier[Int, String, java.lang.Integer] =
    new CacheSupplier[Int, String, java.lang.Integer] {
      override def createCache(): Cache[java.lang.Integer, String] =
        CacheBuilder.newBuilder().build[java.lang.Integer, String]()

      override def getKey(input: Int): java.lang.Integer = input
    }

  class MyDoFn extends ScalaAsyncLookupDoFn[Int, String, Client](100, cacheSupplier) {

    override def asyncLookup(client: Client, input: Int): Future[String] = ???

    override def newClient(): Client = ???
  }

  class Client {
    implicit val es: ExecutionContextExecutorService =
      ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(8))

    // Process a request asynchronously
    def request(input: Int): Future[String] = Future {
      Thread.sleep(100)
      Client.requestCount.incrementAndGet()
      Client.map(input)
    }
  }

  object Client {
    val requestCount: AtomicInteger = new AtomicInteger(0)

    val map: Map[Int, String] = Map(1 -> "one", 2 -> "two", 3 -> "three", 4 -> "four", 5 -> "five")
  }
}
