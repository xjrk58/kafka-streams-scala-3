/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.scala.serialization

import com.sksamuel.avro4s.{Decoder, Encoder, SchemaFor}
import io.confluent.kafka.serializers.{AbstractKafkaSchemaSerDeConfig, KafkaAvroDeserializer, KafkaAvroSerializer}
import org.apache.avro.generic.{GenericRecord, IndexedRecord}
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer, Serdes => JSerdes}

import java.util
import scala.jdk.CollectionConverters._

/**
 * Serdes backed by the Confluent Schema Registry wire format, mapping between Scala case classes
 * and Avro records via avro4s.
 *
 * The returned serdes are configured eagerly from the given registry URL and properties; the
 * `configure` hook of the returned serde is a no-op, so they are meant to be passed explicitly
 * (or implicitly, in the style of this module) rather than set as `default.key/value.serde`
 * class names.
 *
 * {{{
 * implicit val orderSerde: Serde[Order] = AvroSerdes.caseClass[Order]("http://schema-registry:8081")
 *
 * builder.stream[String, Order]("orders")
 * }}}
 *
 * For tests, Confluent's in-memory registry is available through `mock://` URLs, e.g.
 * `AvroSerdes.caseClass[Order]("mock://my-test")`.
 */
object AvroSerdes {

  /**
   * A serde for a case class `T`, stored as an Avro record under the subject derived from the
   * topic name (TopicNameStrategy unless overridden via `properties`). The Avro schema is derived
   * from `T` by avro4s and auto-registered by default.
   *
   * @param schemaRegistryUrl the Schema Registry endpoint, e.g. `http://localhost:8081`
   * @param isKey             whether the serde is used for record keys (affects subject naming)
   * @param properties        extra client config, e.g. auth or `auto.register.schemas`
   */
  def caseClass[T >: Null: Encoder: Decoder: SchemaFor](
    schemaRegistryUrl: String,
    isKey: Boolean = false,
    properties: Map[String, AnyRef] = Map.empty
  ): Serde[T] = {
    val format = AvroRecordFormat[T]
    val (serializer, deserializer) = configured(schemaRegistryUrl, isKey, properties)

    JSerdes.serdeFrom(
      new Serializer[T] {
        override def serialize(topic: String, data: T): Array[Byte] =
          if (data == null) null else serializer.serialize(topic, format.to(data))
        override def close(): Unit = serializer.close()
      },
      new Deserializer[T] {
        override def deserialize(topic: String, bytes: Array[Byte]): T =
          if (bytes == null) null
          else format.from(deserializer.deserialize(topic, bytes).asInstanceOf[IndexedRecord])
        override def close(): Unit = deserializer.close()
      }
    )
  }

  /**
   * A pass-through serde for Avro `GenericRecord`s, for topologies that work with dynamic or
   * registry-defined schemas rather than case classes.
   */
  def genericRecord(
    schemaRegistryUrl: String,
    isKey: Boolean = false,
    properties: Map[String, AnyRef] = Map.empty
  ): Serde[GenericRecord] = {
    val (serializer, deserializer) = configured(schemaRegistryUrl, isKey, properties)

    JSerdes.serdeFrom(
      new Serializer[GenericRecord] {
        override def serialize(topic: String, data: GenericRecord): Array[Byte] = serializer.serialize(topic, data)
        override def close(): Unit = serializer.close()
      },
      new Deserializer[GenericRecord] {
        override def deserialize(topic: String, bytes: Array[Byte]): GenericRecord =
          deserializer.deserialize(topic, bytes).asInstanceOf[GenericRecord]
        override def close(): Unit = deserializer.close()
      }
    )
  }

  private def configured(
    schemaRegistryUrl: String,
    isKey: Boolean,
    properties: Map[String, AnyRef]
  ): (KafkaAvroSerializer, KafkaAvroDeserializer) = {
    val config: util.Map[String, AnyRef] =
      (properties + (AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> schemaRegistryUrl)).asJava
    val serializer = new KafkaAvroSerializer()
    serializer.configure(config, isKey)
    val deserializer = new KafkaAvroDeserializer()
    deserializer.configure(config, isKey)
    (serializer, deserializer)
  }
}
