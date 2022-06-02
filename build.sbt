ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

lazy val root = (project in file("."))
  .settings(
    name := "bi-zone-exercise"
  )
val zioVersion = "1.0.12"
libraryDependencies ++= Seq(
  "dev.zio" %% "zio" % zioVersion,
  "dev.zio" %% "zio-streams" % zioVersion,
  "dev.zio" %% "zio-test" % zioVersion % "test",
  "dev.zio" %% "zio-test-sbt" % zioVersion % "test"
)

testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
