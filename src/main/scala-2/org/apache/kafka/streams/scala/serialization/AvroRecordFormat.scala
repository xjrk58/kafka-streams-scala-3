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

import com.sksamuel.avro4s.{Decoder, Encoder, RecordFormat, SchemaFor}

/**
 * Version bridge: summons an avro4s `RecordFormat` from the Encoder/Decoder/SchemaFor context
 * bounds. avro4s 4.x (Scala 2) and 5.x (Scala 3) construct `RecordFormat` differently.
 */
private[serialization] object AvroRecordFormat {
  def apply[T: Encoder: Decoder: SchemaFor]: RecordFormat[T] = RecordFormat[T]
}
