import com.jsuereth.sbtpgp.PgpKeys.publishSigned

Global / onChangedBuildSource := ReloadOnSourceChanges

inThisBuild(
  List(
    organization := "io.getquill",
    homepage := Some(url("https://zio.dev/zio-protoquill")),
    licenses := List(("Apache License 2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))),
    developers := List(
      Developer("deusaquilus", "Alexander Ioffe", "", url("https://github.com/deusaquilus"))
    ),
    scmInfo := Some(
      ScmInfo(url("https://github.com/zio/zio-protoquill"), "git:git@github.com:zio/zio-protoquill.git")
    ),
    versionScheme := Some("always"),
  )
)

addCommandAlias("runCommunityBuild", "; quill-sql/test; quill-sql-tests/test")
addCommandAlias("fmt", "all scalafmt test:scalafmt")

val isCommunityBuild =
  sys.props.getOrElse("community", "false").toBoolean

val isCommunityRemoteBuild =
  sys.props.getOrElse("communityRemote", "false").toBoolean

lazy val scalatestVersion =
  if (isCommunityRemoteBuild) "3.2.7" else "3.2.18"

lazy val baseModules = Seq[sbt.ClasspathDep[sbt.ProjectReference]](
  `quill-sql`
)

lazy val sqlTestModules = Seq[sbt.ClasspathDep[sbt.ProjectReference]](
  `quill-sql-tests`
)

lazy val dbModules = Seq[sbt.ClasspathDep[sbt.ProjectReference]](
  `quill-jooq`
)

lazy val allModules =
  baseModules ++ sqlTestModules ++ dbModules

lazy val communityBuildModules =
  Seq[sbt.ClasspathDep[sbt.ProjectReference]](
    `quill-sql`, `quill-sql-tests`
  )

val filteredModules = {
  val modulesStr = sys.props.get("modules")
  println(s"SBT =:> Modules Argument Value: ${modulesStr}. community=${isCommunityBuild}, communityRemote=${isCommunityRemoteBuild}")

  val selectedModules = modulesStr match {
    case _ if (isCommunityBuild) =>
      println("SBT =:> Doing Community Build! Filtering Community-Build Modules Only")
      communityBuildModules
    case Some("base") =>
      println("SBT =:> Compiling Base Modules")
      baseModules
    case Some("sqltest") =>
      println("SBT =:> Compiling SQL test Modules")
      sqlTestModules
    case Some("db") =>
      println("SBT =:> Compiling Database Modules (jOOQ)")
      dbModules
    case Some("none") =>
      println("SBT =:> Invoking Aggregate Project")
      Seq[sbt.ClasspathDep[sbt.ProjectReference]]()
    case _ =>
      println("SBT =:> No Modules Switch Specified, Compiling All Modules by Default")
      allModules
  }

  println(s"=== Selected Modules ===\n${selectedModules.map(_.project.toString).toList.mkString("\n")}\n=== End Selected Modules ===")
  selectedModules
}

val zioQuillVersion = "4.8.5"
val zioVersion = "2.1.20"

lazy val `quill` =
  (project in file("."))
    .settings(commonSettings: _*)
    .settings(
      publishArtifact := false,
      publish / skip := true,
      publishLocal / skip := true,
      publishSigned / skip := true,
      crossScalaVersions := Nil, // https://www.scala-sbt.org/1.x/docs/Cross-Build.html#Cross+building+a+project+statefully
    )
    .aggregate(filteredModules.map(_.project): _*)

lazy val `quill-sql` =
  (project in file("quill-sql"))
    .settings(commonSettings: _*)
    .settings(
      resolvers ++= Seq(
        Resolver.mavenLocal,
        "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
        "Sonatype OSS Releases" at "https://oss.sonatype.org/content/repositories/releases"
      ),
      excludeDependencies ++= Seq(
        "com.typesafe.scala-logging" % "scala-logging_2.13"
      ),
      libraryDependencies ++= Seq(
        // Needs to be in-sync with both quill-engine and scalafmt-core or ClassNotFound
        // errors will happen. Even if the pprint classes are actually there
        "io.suzaku" %% "boopickle" % "1.5.0",
        "com.lihaoyi" %% "pprint" % "0.9.3",
        "ch.qos.logback" % "logback-classic" % "1.5.18" % Test,
        "io.getquill" %% "quill-engine" % zioQuillVersion,
        "dev.zio" %% "zio" % zioVersion,
        ("io.getquill" %% "quill-util" % zioQuillVersion)
          .excludeAll({
            if (isCommunityBuild)
              Seq(ExclusionRule(organization = "org.scalameta", name = "scalafmt-core_2.13"))
            else
              Seq.empty
          }: _*),
        "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",
        "org.scalatest" %% "scalatest" % scalatestVersion % Test,
        "org.scalatest" %% "scalatest-mustmatchers" % scalatestVersion % Test,
        "com.vladsch.flexmark" % "flexmark-all" % "0.64.8" % Test
      )
    )

// Moving heavy tests to separate module so it can be compiled in parallel with others
lazy val `quill-sql-tests` =
  (project in file("quill-sql-tests"))
    .settings(publish / skip := true)
    .settings(commonSettings: _*)
    .settings(
      Test / testOptions += Tests.Argument("-oF")
    )
    .dependsOn(`quill-sql` % "compile->compile;test->test")

//lazy val `quill-sql-all` = (project in file(".")).aggregate(`quill-sql`, `quill-sql-tests`)

lazy val jooqVersion = "3.19.16"

lazy val `quill-jooq` =
  (project in file("quill-jooq"))
    .settings(commonSettings: _*)
    .settings(
      Test / fork := true,
      libraryDependencies ++= Seq(
        "org.jooq" % "jooq" % jooqVersion,
        "dev.zio" %% "zio" % zioVersion,
        "com.zaxxer" % "HikariCP" % "6.3.2" exclude("org.slf4j", "*"),
        "org.postgresql" % "postgresql" % "42.7.7" % Test,
        "com.h2database" % "h2" % "2.3.232" % Test,
        "org.xerial" % "sqlite-jdbc" % "3.50.3.0" % Test,
        "ch.qos.logback" % "logback-classic" % "1.5.18" % Test,
        "org.scalatest" %% "scalatest" % scalatestVersion % Test,
        "org.scalatest" %% "scalatest-mustmatchers" % scalatestVersion % Test
      )
    )
    .dependsOn(`quill-sql` % "compile->compile;test->test")

// Include scalafmt formatter for pretty printing failed queries
val includeFormatter =
  sys.props.getOrElse("formatScala", "false").toBoolean

lazy val commonSettings =
  basicSettings ++ {
    if (isCommunityRemoteBuild)
      Seq(
        Compile / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat
      )
    else
      Seq.empty
  }

lazy val basicSettings = Seq(
  Test / testOptions += Tests.Argument("-oI"),
  libraryDependencies ++= Seq(
    ("org.scala-lang.modules" %% "scala-java8-compat" % "1.0.2")
  ),
  excludeDependencies ++= Seq(
    ExclusionRule("org.scala-lang.modules", "scala-collection-compat_2.13")
  ),
  scalaVersion := "3.3.6",
  // The -e option is the 'error' report of ScalaTest. We want it to only make a log
  // of the failed tests once all tests are done, the regular -o log shows everything else.
  // Test / testOptions ++= Seq(
  //   Tests.Argument(TestFrameworks.ScalaTest, "-oF")
  //   //  /*, "-eGNCXEHLOPQRM"*/, "-h", "target/html", "-u", "target/junit"
  //   //Tests.Argument(TestFrameworks.ScalaTest, "-u", "junits")
  //   //Tests.Argument(TestFrameworks.ScalaTest, "-h", "testresults")
  // ),
  scalacOptions ++= Seq(
    "-language:implicitConversions", "-explain",
    // See https://docs.scala-lang.org/scala3/guides/migration/tooling-syntax-rewriting.html
    "-no-indent",
    "-release:11",
  ),
  javacOptions := Seq("-source", "11", "-target", "11"),
)

// force redraft
// force redraft
