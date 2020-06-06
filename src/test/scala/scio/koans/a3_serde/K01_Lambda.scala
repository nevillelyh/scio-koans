package scio.koans.a3_serde

import scio.koans.shared._

/**
 * Serialize a simple lambda.
 */
class K01_Lambda extends Koan {
  ImNotDone

  "K01a" should "be serializable" in {
    val obj = SerializableUtils.roundTrip((new K01a).plusOne _)
    obj(0) shouldBe 1
  }

  "K01b" should "be serializable" in {
    val obj = SerializableUtils.roundTrip((new K01b).plusOne _)
    obj(0) shouldBe 1
  }
}

class K01a extends Serializable {
  def plusOne(x: Int): Int = x + 1
}

class K01b {
  def plusOne(x: Int): Int = x + 1
}
