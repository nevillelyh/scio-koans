package scio.koans.a3_serde

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import scio.koans.shared._

/**
 * Serialize a simple object.
 */
class K00_Serializable extends Koan {
  ImNotDone

  "K00a" should "be serializable" in {
    val obj = SerializableUtils.roundTrip(new K00a)
    obj.x shouldBe 0
  }

  "K00b" should "be serializable" in {
    val obj = SerializableUtils.roundTrip(new K00b)
    obj.x shouldBe 0
  }
}

// A class must extend `Serializable` to support Java serialization
class K00a extends Serializable {
  val x = 0
}

class K00b {
  val x = 0
}

// Serialize and deserialize using Java serialization
object SerializableUtils {
  def encode[T](v: T): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(v)
    oos.close()
    baos.toByteArray
  }

  def decode[T](v: Array[Byte]): T = {
    val bais = new ByteArrayInputStream(v)
    val ois = new ObjectInputStream(bais)
    val obj = ois.readObject().asInstanceOf[T]
    ois.close()
    obj
  }

  def roundTrip[T](v: T): T = decode(encode(v))
}
