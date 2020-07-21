name := "scio-koans"
description := "Scio Koans"

val scioVersion = "0.9.2"
val magnolifyVersion = "0.2.2"
val scalaTestVersion = "3.2.0"

val allKoans = taskKey[Seq[(String, Boolean)]]("Show all Koans")
val nextKoan = taskKey[Unit]("Run next Koan")
val verifyKoans = taskKey[Unit]("Verify all Koans")

val Cyan = Some(scala.Console.CYAN)
val Green = Some(scala.Console.GREEN)
val Red = Some(scala.Console.RED)

val commonSettings = Seq(
  organization := "me.lyh",
  scalaVersion := "2.12.12",
  crossScalaVersions := Seq("2.12.12"),
  scalacOptions ++= Seq(
    "-target:jvm-1.8",
    "-deprecation",
    "-feature",
    "-unchecked",
    "-language:higherKinds"
  ),
  Test / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat,
  allKoans := {
    val classLoader = (Test / testLoader).value
    val tests = (Test / definedTests).value
    val ks = getAllKoans(classLoader, tests)
    showAllKoans(ks)
    ks
  },
  allKoans := allKoans.dependsOn(Test / testLoader, Test / definedTests).value,
  nextKoan := {
    val ks = allKoans.value
    // Find first test with `done == false`
    ks.indexWhere(!_._2) match {
      case -1 => ConsoleLogger().info("All koans completed")
      case n =>
        val name = ks(n)._1
        val logger = ConsoleLogger()
        logger.info(Def.withColor(s"Running Koan ${n + 1} of ${ks.size}: $name", Cyan))
        val s = state.value
        Project.extract(s).runInputTask(Test / testOnly, " " + name, s)
        // If the test passes
        logger.info(Def.withColor("Koan completed", Green))
        logger.info(Def.withColor("Remove `ImNotDone` to move on to the next one", Green))
    }
    Unit
  },
  nextKoan := nextKoan.dependsOn(allKoans, state).value,
  verifyKoans := {
    val ks = allKoans.value
    val completed = ks.filter(_._2)
    if (completed.nonEmpty) {
      val logger = ConsoleLogger()
      logger.error("Not all tests are pending, did you forget `ImNotDone`?")
      println(completed)
      completed.foreach(kv => logger.error(s"  ${kv._1}"))
      throw new AlreadyHandledException(new RuntimeException)
    }
  },
  verifyKoans := verifyKoans.dependsOn(allKoans).value,
)

val jmhSettings = Seq(
  Jmh / classDirectory := (Test / classDirectory).value,
  Jmh / dependencyClasspath := (Test / dependencyClasspath).value,
  // rewire tasks, so that 'test' automatically invokes 'jmh:compile'
  // (otherwise a clean 'test' would fail)
  Jmh / compile := (Jmh / compile).dependsOn(Test / compile).value,
  Test / test := (Test / test).dependsOn(Jmh / compile).value,
  Test / testOnly := (Test / testOnly).dependsOn(Jmh / compile).evaluated,
  Test / parallelExecution := false
)

val root: Project = Project(
  "scio-koans",
  file(".")
).settings(
    commonSettings ++ jmhSettings,
    libraryDependencies ++= Seq(
      "com.spotify" %% "scio-core" % scioVersion,
      "com.spotify" %% "scio-test" % scioVersion % Test,
      "com.spotify" %% "magnolify-cats" % magnolifyVersion
    )
  )
  .enablePlugins(JmhPlugin)

def getAllKoans(classLoader: ClassLoader, tests: Seq[TestDefinition]): Seq[(String, Boolean)] = {
  tests.iterator
    .flatMap { td =>
      val cls = classLoader.loadClass(td.name)
      val koan = cls.newInstance()
      //  Extract `Koan#done`
      cls.getMethods
        .find { m =>
          m.getName.contains("done") &&
          m.getParameterCount == 0 &&
          m.getReturnType == classOf[Boolean]
        }
        .map(_.invoke(koan).asInstanceOf[Boolean])
        .map(td.name -> _)
    }
    .toList
    .sortBy(_._1) // Sort all koans alphabetically
}

def showAllKoans(koans: Seq[(String, Boolean)]): Unit = {
  val logger = ConsoleLogger()
  val completed = koans.count(_._2)
  val total = koans.size
  logger.info(Def.withColor(s"Available Koans: $completed of $total completed", Cyan))
  koans.foreach { kv =>
    val (color, status) = if (kv._2) (Green, "completed") else (Red, "pending")
    val msg = s"- %-40s\t: %s".format(kv._1, status)
    logger.info(Def.withColor(msg, color))
  }
}
