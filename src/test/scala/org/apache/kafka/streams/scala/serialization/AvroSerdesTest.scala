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

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.apache.kafka.streams.scala.utils.TestDriver
import org.junit.jupiter.api.Assertions.{assertEquals, assertNull, assertTrue}
import org.junit.jupiter.api.Test

case class Order(id: String, amount: Double, note: Option[String], tags: List[String])

class AvroSerdesTest extends TestDriver {

  @Test
  def testCaseClassRoundTrip(): Unit = {
    val serde = AvroSerdes.caseClass[Order]("mock://case-class-round-trip")
    val order = Order("o-1", 12.5, Some("gift"), List("summer", "sale"))

    val bytes = serde.serializer.serialize("orders", order)
    assertEquals(order, serde.deserializer.deserialize("orders", bytes))
  }

  @Test
  def testCaseClassNullPassthrough(): Unit = {
    val serde = AvroSerdes.caseClass[Order]("mock://case-class-null")

    assertNull(serde.serializer.serialize("orders", null))
    assertNull(serde.deserializer.deserialize("orders", null))
  }

  @Test
  def testSchemaRegisteredUnderTopicSubject(): Unit = {
    val valueSerde = AvroSerdes.caseClass[Order]("mock://subject-naming")
    val keySerde = AvroSerdes.caseClass[Order]("mock://subject-naming", isKey = true)
    val order = Order("o-1", 1.0, None, Nil)

    valueSerde.serializer.serialize("orders", order)
    keySerde.serializer.serialize("orders", order)

    val registry = MockSchemaRegistry.getClientForScope("subject-naming")
    assertTrue(registry.getAllSubjects.contains("orders-value"))
    assertTrue(registry.getAllSubjects.contains("orders-key"))
  }

  @Test
  def testGenericRecordRoundTrip(): Unit = {
    val serde = AvroSerdes.genericRecord("mock://generic-round-trip")
    val schema = SchemaBuilder.record("Person").fields().requiredString("name").endRecord()
    val person = new GenericData.Record(schema)
    person.put("name", "Jiri")

    val bytes = serde.serializer.serialize("people", person)
    assertEquals("Jiri", serde.deserializer.deserialize("people", bytes).get("name").toString)
  }

  @Test
  def testCaseClassSerdeInTopology(): Unit = {
    implicit val orderSerde: Serde[Order] = AvroSerdes.caseClass[Order]("mock://topology")

    val builder = new StreamsBuilder()
    val sourceTopic = "orders"
    val sinkTopic = "amounts"

    builder.stream[String, Order](sourceTopic).mapValues(_.amount).to(sinkTopic)

    val testDriver = createTestDriver(builder)
    val testInput = testDriver.createInput[String, Order](sourceTopic)
    val testOutput = testDriver.createOutput[String, Double](sinkTopic)

    testInput.pipeInput("o-1", Order("o-1", 12.5, Some("gift"), List("summer")))
    assertEquals(12.5, testOutput.readValue, 0.0)
    assertTrue(testOutput.isEmpty)

    testDriver.close()
  }
}
