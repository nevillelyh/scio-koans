package scio.koans.shared

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Base trait for all Koans.
 */
trait Koan extends AnyFlatSpec with Matchers {
  private var done = true

  /**
   * Indicate that you are not done with a Koan.
   *
   * This should be the first line in a Koan. Remove once the test is green and you are satisfied
   * with the solution.
   */
  def ImNotDone: Unit = done = false
}
