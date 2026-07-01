# kafka-streams-scala-3

[![CI](https://github.com/xjrk58/kafka-streams-scala-3/actions/workflows/ci.yml/badge.svg)](https://github.com/xjrk58/kafka-streams-scala-3/actions/workflows/ci.yml)

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

```scala
libraryDependencies += "io.github.xjrk58" %% "kafka-streams-scala" % "<version>"
```

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

## Avro serdes (avro4s + Confluent Schema Registry)

Beyond the vendored upstream code, the module adds `AvroSerdes`: serdes using the
Confluent Schema Registry wire format, with case classes mapped to Avro records by
[avro4s](https://github.com/sksamuel/avro4s) (4.x on Scala 2.13, 5.x on Scala 3 —
a small version bridge in `src/main/scala-2` / `scala-3` hides the API difference).

```scala
import org.apache.kafka.streams.scala.serialization.AvroSerdes

case class Order(id: String, amount: Double, note: Option[String], tags: List[String])

// schema is derived from the case class and auto-registered under <topic>-value
implicit val orderSerde: Serde[Order] = AvroSerdes.caseClass[Order]("http://schema-registry:8081")

builder.stream[String, Order]("orders")
```

- `AvroSerdes.caseClass[T](url, isKey, properties)` — case class serde; pass
  `isKey = true` for key serdes (subject naming), and extra registry client config
  (auth, `auto.register.schemas`, subject name strategy, …) via `properties`.
- `AvroSerdes.genericRecord(url, isKey, properties)` — pass-through serde for
  `org.apache.avro.generic.GenericRecord`.

The serdes are configured eagerly from the constructor arguments (the serde's own
`configure` hook is a no-op), so pass them explicitly or implicitly rather than as
`default.value.serde` class names. In tests, Confluent's in-memory registry works
out of the box via `mock://` URLs: `AvroSerdes.caseClass[Order]("mock://my-test")`.

## Build

```
sbt +compile    # compile for 2.13 and 3
sbt +test       # run the test suite on both
sbt +package    # build jars for both
```

## CI and releasing

GitHub Actions runs formatting checks (`scalafmtCheckAll`, using Apache Kafka's
scalafmt config) and the cross-built test suite on every push and PR.

Releases are published to **Maven Central** (Central Portal) by `sbt-ci-release`:

- pushing a `v*` tag publishes that release — versions track the wrapped
  `kafka-streams` version, e.g. `v4.3.0` → `4.3.0` (use `v4.3.0.1` etc. for
  module-only fixes);
- pushes to `master` publish `-SNAPSHOT` versions.

The workflow needs four repository secrets (see the
[sbt-ci-release docs](https://github.com/sbt/sbt-ci-release#secrets)):
`PGP_SECRET`, `PGP_PASSPHRASE` (base64-encoded GPG signing key), and
`SONATYPE_USERNAME`, `SONATYPE_PASSWORD` (a Central Portal *user token* for an
account that owns the `io.github.xjrk58` namespace).

## Maintenance notes

- When bumping to Kafka 5.x, update `kafkaVersion` in `build.sbt` and expect
  small mechanical fixes if the Java Streams API surface changed.
- The `org.apache.kafka.streams.scala` *package* name is kept for drop-in
  compatibility, which is fine under the Apache-2.0 terms (attribution is in
  `NOTICE`); the artifact is published under the `io.github.xjrk58` groupId
  because `org.apache.kafka` belongs to the ASF.

## Local modifications vs upstream

1. Removed 20 KIP-1244 `@deprecated(..., "4.3.0")` annotations.
2. `serialization/Serdes.scala`: parenthesized two lambda parameters (required by Scala 3).
3. `kstream/KTableTest.scala`: type-ascribed two `ValueJoiner` lambdas (Scala 3
   does not SAM-convert them during overload resolution).
4. `utils/TestDriver.scala`: replaced `org.apache.kafka.test.TestUtils.tempDirectory()`
   with `java.nio.file.Files.createTempDirectory` to avoid depending on the
   kafka-clients test-jar.
