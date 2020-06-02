package scio.koans.shared

import com.spotify.scio._
import com.spotify.scio.testing._

import scala.language.implicitConversions

/**
 * Base Koan for Scio pipelines.
 */
abstract class PipelineKoan extends Koan with PipelineSpec

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
