package scio.koans.shared

import java.io.PrintStream
import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations._
import org.openjdk.jmh.runner.format.OutputFormatFactory
import org.openjdk.jmh.runner.options._
import org.openjdk.jmh.runner.{BenchmarkException, Runner, RunnerException}
import org.openjdk.jmh.util.NullOutputStream

import scala.collection.JavaConverters._

/**
 * Base Koan for JMH micro-benchmarks.
 */
@State(Scope.Thread)
abstract class JmhKoan extends Koan {
  // Map of benchmark labels to (ns/op, result)
  protected lazy val results: Map[String, (Double, Any)] =
    try {
      // Special settings so that benchmarks can run quickly in ScalaTest
      // Do not use for not real benchmark
      val options = new OptionsBuilder()
        .include(this.getClass.getName + ".*")
        .mode(Mode.AverageTime)
        .threads(1)
        .forks(0)
        .warmupIterations(0)
        .measurementIterations(1)
        .measurementTime(TimeValue.seconds(1))
        .timeUnit(TimeUnit.NANOSECONDS)
        .shouldFailOnError(true)
        .build()

      // Suppress JMH output
      val format = OutputFormatFactory.createFormatInstance(
        new PrintStream(new NullOutputStream),
        VerboseMode.SILENT
      )

      val runner = new Runner(options, format)

      runner
        .run()
        .asScala
        .iterator
        .map(_.getPrimaryResult)
        .map { r =>
          val label = r.getLabel
          val result = this.getClass.getMethod(label).invoke(this)
          label -> (r.getScore, result)
        }
        .toMap
    } catch {
      // Surface root cause
      case re: RunnerException =>
        re.getCause match {
          case be: BenchmarkException =>
            if (be.getSuppressed.length == 1) {
              throw be.getSuppressed.head
            } else {
              throw be
            }
          case e: Throwable => throw e
        }
      case e: Throwable => throw e
    }

  // All `@Benchmark def`s excluding `baseline`
  private val labels: Seq[String] =
    // Skip JMH generated classes
    if (this.getClass.getSimpleName.endsWith("_jmhType")) {
      Nil
    } else {
      val b = this.getClass.getDeclaredMethods.iterator
        .filter(_.getAnnotation(classOf[Benchmark]) != null)
        .map(_.getName)
        .toSeq
        .sorted
      require(b.contains("baseline"), "Missing benchmark baseline")
      b.filterNot(_ == "baseline")
    }

  /**
   * Verify that all benchmark results are the same as `baseline`.
   */
  def verifyResults(): Unit =
    "Revisions" should "return the same result as baseline" in {
      labels.foreach { label =>
        withClue(s"Benchmark $label:") {
          results("baseline")._2 shouldBe results(label)._2
        }
      }
    }

  /**
   * Verify that all benchmark scores are faster than `baseline`.
   */
  def verifySpeedup(speedup: Speedup): Unit = {
    val (factor, cmp) = speedup match {
      case Speedup.Faster =>
        ("faster than", (s0: Double, s1: Double) => s0 should be >= s1)
      case Speedup.Percent(x) =>
        (s"$x% faster than", (s0: Double, s1: Double) => ((s0 - s1) / s0 * 100) should be >= x)
      case Speedup.Times(x) =>
        (s"${x} times faster than", (s0: Double, s1: Double) => (s0 / s1) should be >= x.toDouble)
      case Speedup.Error(x) =>
        (
          s"+/- ${x}% from",
          (s0: Double, s1: Double) => math.abs((s0 - s1) / s0 * 100) should be <= x.toDouble
        )
    }

    they should s"be $factor baseline" in {
      val s0 = results("baseline")._1

      print(scala.Console.CYAN)
      println("%-10s: %16s".format("Label", "Result"))
      println("%-10s: %16.3f ns/op".format("baseline", s0))

      val inCI = sys.env.get("CI").contains("true")
      labels.foreach { label =>
        val s1 = results(label)._1
        println("%-10s: %16.3f ns/op".format(label, s1))
        if (!inCI) {
          withClue(s"Benchmark $label:")(cmp(s0, s1))
        }
      }
      if (inCI) {
        print(scala.Console.YELLOW)
        println("Speedups ignored in CI")
      }
      print(scala.Console.RESET)
    }
  }
}

/**
 * Represent benchmark speedup vs. baseline.
 */
sealed trait Speedup

object Speedup {

  /**
   * New benchmark is faster than baseline.
   */
  case object Faster extends Speedup

  /**
   * New benchmark is `x`% faster than baseline.
   */
  case class Percent(x: Double) extends Speedup

  /**
   * New benchmark is `x` times faster than baseline.
   */
  case class Times(x: Int) extends Speedup

  /**
   * New delta is within `x`% of baseline.
   */
  case class Error(x: Double) extends Speedup
}
