package scio.koans.a3_serde

import com.google.common.base.Splitter
import org.apache.avro.Schema
import scio.koans.avro.Test
import scio.koans.shared._

import scala.collection.JavaConverters._

/**
 * Serialize a simple object with non-serializable member.
 */
class K02_AvroSchema extends Koan {
  ImNotDone

  "K02a" should "be serializable" in {
    val obj = SerializableUtils.roundTrip(new K02a(" "))
    val result = obj.splitter.split("a b c").asScala
    result should contain theSameElementsAs Seq("a", "b", "c")
  }

  "K02b" should "be serializable" in {
    val obj = SerializableUtils.roundTrip(new K02b(Test.getClassSchema))
    obj.schema shouldBe Test.getClassSchema
  }
}

class K02a(val separator: String) extends Serializable {

  /**
   * - `Splitter` is not `Serializable`
   * - `String` parameter `separator` is
   * - `@transient` so that the field is not serialized
   * - `lazy` so that the field is initialized on demand
   */
  @transient lazy val splitter: Splitter = Splitter.on(separator)
}

// Hint: `Schema` is not serializable but `String` is
class K02b(val schema: Schema) extends Serializable {}
