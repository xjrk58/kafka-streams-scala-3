// Vendored fork of Apache Kafka's `streams-scala` module (removed upstream in
// Kafka 5.0 per KIP-1244), cross-built for Scala 2.13 and Scala 3.
// Sources taken from the apache/kafka 4.3 branch with the KIP-1244
// module-level @deprecated annotations removed.

val scala213 = "2.13.16"
val scala3   = "3.3.6"

val kafkaVersion = "4.3.0"

ThisBuild / organization := "dev.gra" // TODO: change to your own groupId before publishing
ThisBuild / version      := kafkaVersion // tracks the wrapped kafka-streams version
ThisBuild / licenses     := Seq("Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0"))

lazy val root = (project in file("."))
  .settings(
    name               := "kafka-streams-scala",
    scalaVersion       := scala213,
    crossScalaVersions := Seq(scala213, scala3),
    scalacOptions ++= Seq(
      "-deprecation",
      "-feature",
      "-unchecked",
      "-language:implicitConversions"
    ),
    libraryDependencies ++= Seq(
      "org.apache.kafka"    % "kafka-streams"            % kafkaVersion,
      "org.apache.kafka"    % "kafka-streams-test-utils" % kafkaVersion                    % Test,
      "org.junit.jupiter"   % "junit-jupiter"            % "5.10.3"                        % Test,
      "org.mockito"         % "mockito-core"             % "5.14.2"                        % Test,
      "org.mockito"         % "mockito-junit-jupiter"    % "5.14.2"                        % Test,
      "com.github.sbt.junit" % "jupiter-interface"       % JupiterKeys.jupiterVersion.value % Test,
      "org.slf4j"           % "slf4j-simple"             % "2.0.16"                        % Test
    ),
    Test / fork := true,
    Test / javaOptions += "-Dorg.slf4j.simpleLogger.defaultLogLevel=error"
  )
