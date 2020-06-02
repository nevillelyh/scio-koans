package scio.koans.a2_coders

import scio.koans.avro.Test
import scio.koans.shared._
import com.spotify.scio.coders._
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.apache.beam.sdk.{coders => beam}
import org.apache.beam.sdk.util.CoderUtils
import org.openjdk.jmh.annotations._

import scala.collection.mutable

/**
 * Encode Avro specific and generic records.
 */
class K00_Avro extends JmhKoan {
  ImNotDone

  val specificRecord: Test = Test
    .newBuilder()
    .setInt1(1)
    .setInt2(2)
    .setInt3(3)
    .setInt4(4)
    .setInt5(5)
    .setStr1("a")
    .setStr2("b")
    .setStr3("c")
    .setStr4("d")
    .setStr5("e")
    .build()

  val genericRecord: GenericRecord = new GenericRecordBuilder(Test.getClassSchema)
    .set("int1", 1)
    .set("int2", 2)
    .set("int3", 3)
    .set("int4", 4)
    .set("int5", 5)
    .set("str1", "a")
    .set("str2", "b")
    .set("str3", "c")
    .set("str4", "d")
    .set("str5", "e")
    .build()

  // `com.spotify.scio.coders.Coder[T]` is an abstraction derived at compile time
  // https://spotify.github.io/scio/internals/Coders.html
  val scioSpecific: Coder[Test] = Coder[Test]

  // A Scio coder is materialized as a Beam coder
  // Beam coders handles the actual encoding and decoding of data
  // https://beam.apache.org/documentation/programming-guide/#data-encoding-and-type-safety
  val beamSpecific: beam.Coder[Test] = CoderMaterializer.beamWithDefault(scioSpecific)

  @Benchmark def baseline: mutable.WrappedArray[Byte] =
    CoderUtils.encodeToByteArray(beamSpecific, specificRecord)

  // FIXME: implement this efficiently
  // Hint: look at the compiler warning
  val scioGeneric: Coder[GenericRecord] = Coder[GenericRecord]
  val beamGeneric: beam.Coder[GenericRecord] = CoderMaterializer.beamWithDefault(scioGeneric)

  @Benchmark def generic: mutable.WrappedArray[Byte] =
    CoderUtils.encodeToByteArray(beamGeneric, genericRecord)

  // Hint: this fails because the generic record is encoded with schema and significantly larger
  verifyResults()

  // Efficient encoding generic/specific record of the same schema should be comparable
  verifySpeedup(Speedup.Error(25))

  it should "encode to the same size as baseline" in {
    baseline.size shouldBe generic.size
  }
}
