// Vendored fork of Apache Kafka's `streams-scala` module (removed upstream in
// Kafka 5.0 per KIP-1244), cross-built for Scala 2.13 and Scala 3.
// Sources taken from the apache/kafka 4.3 branch with the KIP-1244
// module-level @deprecated annotations removed.

val scala213 = "2.13.16"
val scala3 = "3.3.6"

val kafkaVersion = "4.3.0"

// Version is derived from git tags by sbt-ci-release (sbt-dynver); tag releases as
// v<kafka-version>[.N] to track the wrapped kafka-streams version, e.g. v4.3.0.
ThisBuild / organization := "io.github.xjrk58"
ThisBuild / licenses := Seq("Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0"))
ThisBuild / homepage := Some(url("https://github.com/xjrk58/kafka-streams-scala-3"))
ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/xjrk58/kafka-streams-scala-3"),
    "scm:git:git@github.com:xjrk58/kafka-streams-scala-3.git"
  )
)
ThisBuild / developers := List(
  Developer("xjrk58", "Jiri Syrovy", "jrk@hkfree.org", url("https://github.com/xjrk58"))
)
ThisBuild / versionScheme := Some("semver-spec")

lazy val root = (project in file("."))
  .settings(
    name := "kafka-streams-scala",
    scalaVersion := scala213,
    crossScalaVersions := Seq(scala213, scala3),
    resolvers += "Confluent" at "https://packages.confluent.io/maven/",
    scalacOptions ++= Seq(
      "-deprecation",
      "-feature",
      "-unchecked",
      "-language:implicitConversions"
    ),
    libraryDependencies ++= Seq(
      "org.apache.kafka" % "kafka-streams" % kafkaVersion,
      "io.confluent" % "kafka-avro-serializer" % "8.3.0",
      // avro4s 4.x is the Scala 2 line, 5.x the Scala 3 line; both expose RecordFormat
      "com.sksamuel.avro4s" %% "avro4s-core" % (CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((2, _)) => "4.1.2"
        case _            => "5.0.15"
      }),
      "org.apache.kafka" % "kafka-streams-test-utils" % kafkaVersion % Test,
      "org.junit.jupiter" % "junit-jupiter" % "5.10.3" % Test,
      "org.mockito" % "mockito-core" % "5.14.2" % Test,
      "org.mockito" % "mockito-junit-jupiter" % "5.14.2" % Test,
      "com.github.sbt.junit" % "jupiter-interface" % JupiterKeys.jupiterVersion.value % Test,
      "org.slf4j" % "slf4j-simple" % "2.0.16" % Test
    ),
    Test / fork := true,
    Test / javaOptions += "-Dorg.slf4j.simpleLogger.defaultLogLevel=error"
  )
