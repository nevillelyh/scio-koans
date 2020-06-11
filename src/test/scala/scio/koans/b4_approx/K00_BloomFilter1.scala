package scio.koans.b4_approx

import com.spotify.scio.coders.{Coder, CoderMaterializer}
import org.apache.beam.sdk.util.CoderUtils
import scio.koans.shared._

/**
 * Bloom Filter basics.
 */
class K00_BloomFilter1 extends Koan {
  ImNotDone

  val positives: Seq[String] = (1 to 1000).map("positive-%05d".format(_))
  val negatives: Seq[String] = (1 to 50).map("negative-%05d".format(_))

  "Snippet" should "work" in {
    // Scio 0.9.x Approximate Filter API
    // https://spotify.github.io/scio/migrations/v0.9.0-Migration-Guide.html#bloom-filter
    import com.spotify.scio.hash._
    import magnolify.guava.auto._

    // Scale number of unique items 1000 by 1.1 so we don't saturate the Bloom Filter
    val bf1: BloomFilter[String] = positives.asApproxFilter(BloomFilter, 1100, 0.03)

    // Bloom Filter will not produce false negatives
    positives.forall(bf1.mightContain) shouldBe true

    // Can it produce false positives?
    negatives.forall(bf1.mightContain) shouldBe ?:[Boolean]

    // What's the maximum false positives we expect given `fpp = 0.03`?
    val maxFp: Int = ???
    negatives.count(bf1.mightContain) should be <= maxFp

    // BF with higher capacity
    val bf2: BloomFilter[String] = positives.asApproxFilter(BloomFilter, 2000)
    // BF with lower `fpp`
    val bf3: BloomFilter[String] = positives.asApproxFilter(BloomFilter, 1000, 0.01)

    // Bloom Filters are serializable
    val coder = CoderMaterializer.beamWithDefault(Coder[BloomFilter[String]])
    val size1 = CoderUtils.encodeToByteArray(coder, bf1).length
    val size2 = CoderUtils.encodeToByteArray(coder, bf2).length
    val size3 = CoderUtils.encodeToByteArray(coder, bf3).length

    // Number of bits = `-n * math.log(p) / (math.log(2) * math.log(2))`
    // Where `n` is expected insertions and `p` is false positive probability
    // Which BF needs the least and most space?
    Seq(size1, size2, size3).min shouldBe ?:[Int]
    Seq(size1, size2, size3).max shouldBe ?:[Int]
  }
}
