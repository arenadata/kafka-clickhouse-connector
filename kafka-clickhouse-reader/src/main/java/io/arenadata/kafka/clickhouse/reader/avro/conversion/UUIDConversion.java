/*
 * Copyright © 2021 Kafka Clickhouse Reader
 *
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
package io.arenadata.kafka.clickhouse.reader.avro.conversion;

import io.arenadata.kafka.clickhouse.reader.avro.type.UUIDLogicalType;
import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;

import java.util.UUID;

public class UUIDConversion extends Conversion<UUID> {

  @Override
  public Class<UUID> getConvertedType() {
    return UUID.class;
  }

  @Override
  public String getLogicalTypeName() {
    return UUIDLogicalType.INSTANCE.getName();
  }

  @Override
  public Schema getRecommendedSchema() {
    return UUIDLogicalType.INSTANCE.addToSchema(Schema.create(Schema.Type.STRING));
  }

  @Override
  public CharSequence toCharSequence(UUID value, Schema schema, LogicalType type) {
    return value.toString();
  }

  @Override
  public UUID fromCharSequence(CharSequence value, Schema schema, LogicalType type) {
    return UUID.fromString(value.toString());
  }
}
