# kafka-streams-scala (vendored)

A vendored, cross-built continuation of Apache Kafka's `streams-scala` module —
the idiomatic Scala DSL for Kafka Streams — which was deprecated in Kafka 4.3 and
removed in Kafka 5.0 per
[KIP-1244](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=399278767).

- Sources taken from the [`apache/kafka` 4.3 branch](https://github.com/apache/kafka/tree/4.3/streams/streams-scala)
  (`streams/streams-scala`), Apache License 2.0 (see `LICENSE` / `NOTICE`).
- The KIP-1244 module-level `@deprecated` annotations have been removed; genuine
  API deprecations (e.g. the old `Transformer` API, deprecated since 4.0) are kept.
- Cross-built for **Scala 2.13 and Scala 3** (the upstream module never supported
  Scala 3, which was one of the reasons it was dropped).
- Full upstream test suite included (JUnit 5 + `TopologyTestDriver`), passing on
  both Scala versions.

## Usage

The package name `org.apache.kafka.streams.scala` is kept as-is, so migrating an
existing application is a dependency swap with **no import changes**:

```scala
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.apache.kafka.streams.scala.StreamsBuilder

val builder = new StreamsBuilder()
builder
  .stream[String, String]("input")
  .flatMapValues(_.toLowerCase.split("\\W+"))
  .groupBy((_, word) => word)
  .count()
  .toStream
  .to("output")
```

**Do not** keep the upstream `org.apache.kafka %% kafka-streams-scala` artifact on
the classpath alongside this module — the classes would clash. (Upstream stops
publishing it at Kafka 5.0, so this only matters on 4.x.)

## Build

```
sbt +compile    # compile for 2.13 and 3
sbt +test       # run the test suite on both
sbt +package    # build jars for both
```

## Maintenance notes

- `ThisBuild / version` tracks the `kafka-streams` version the module wraps
  (currently 4.3.0). When bumping to Kafka 5.x, expect small mechanical fixes if
  the Java Streams API surface changed.
- Before publishing anywhere public, change `ThisBuild / organization` in
  `build.sbt` — the `org.apache.kafka` groupId belongs to the ASF. Keeping the
  Java *package* name is fine under the Apache-2.0 terms (attribution is in
  `NOTICE`), but the artifact coordinates must be your own.

## Local modifications vs upstream

1. Removed 20 KIP-1244 `@deprecated(..., "4.3.0")` annotations.
2. `serialization/Serdes.scala`: parenthesized two lambda parameters (required by Scala 3).
3. `kstream/KTableTest.scala`: type-ascribed two `ValueJoiner` lambdas (Scala 3
   does not SAM-convert them during overload resolution).
4. `utils/TestDriver.scala`: replaced `org.apache.kafka.test.TestUtils.tempDirectory()`
   with `java.nio.file.Files.createTempDirectory` to avoid depending on the
   kafka-clients test-jar.
