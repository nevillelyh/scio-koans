package scio.koans.shared

import com.spotify.scio._
import com.spotify.scio.testing._
import org.apache.beam.sdk.options.{PipelineOptions, PipelineOptionsFactory}

import scala.language.implicitConversions

/**
 * Base Koan for Scio pipelines.
 */
abstract class PipelineKoan extends Koan with PipelineSpec {

  /**
   * Run with `ScioContext` and a specific `targetParallelism`.
   */
  def runWithParallelism[T](n: Int)(fn: ScioContext => T): ScioExecutionContext = {
    val options = PipelineOptionsFactory
      .fromArgs(s"--targetParallelism=$n")
      .as(classOf[PipelineOptions])
    val sc = ScioContext(options)
    fn(sc)
    sc.run()
  }
}

/**
 * Base Koan for Scio transforms.
 */
abstract class TransformKoan extends Koan with PipelineSpec {

  /**
   * Transform input type.
   */
  type InT

  /**
   * Transform output type.
   */
  type OutT

  private var prepareFn: ScioContext => InT = _
  private var verifyFn: OutT => Any = _

  /**
   * Prepare input before each test.
   */
  def prepare(f: ScioContext => InT): Unit = prepareFn = f

  /**
   * Verify output after each test.
   */
  def verify(f: OutT => Any): Unit = verifyFn = f

  /**
   * Add a new test.
   */
  def test(name: String)(f: InT => OutT): Unit = {
    name should "work" in {
      runWithContext { sc =>
        val in = prepareFn(sc)
        val out = f(in)
        verifyFn(out)
      }
    }
  }

  def baseline(f: InT => OutT): Unit = test("baseline")(f)
}
