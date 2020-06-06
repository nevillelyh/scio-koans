package scio.koans.a3_serde

import scio.koans.shared._

import scala.collection.JavaConverters._

/**
 * Serialize a simple object with non-serializable member.
 */
class K03_Static extends Koan {
  ImNotDone

  import K03_Static._

  // Hint: `class K03_Static` includes non-serializable members from ScalaTest and is not fixable
  def plusTwo(x: Int): Int = x + 2

  "plusOne" should "be serializable" in {
    val obj = SerializableUtils.roundTrip(plusOne _)
    obj(0) shouldBe 1
  }

  "plusTwo" should "be serializable" in {
    val obj = SerializableUtils.roundTrip(plusTwo _)
    obj(0) shouldBe 2
  }
}

object K03_Static {
  // Members of `object X` are static and initialized at class loading time
  // They do not go through serialization
  def plusOne(x: Int): Int = x + 1
}
