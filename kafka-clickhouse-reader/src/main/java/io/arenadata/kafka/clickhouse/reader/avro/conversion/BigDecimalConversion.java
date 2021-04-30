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

import io.arenadata.kafka.clickhouse.reader.avro.type.BigDecimalLogicalType;
import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;

import java.math.BigDecimal;

public class BigDecimalConversion extends Conversion<BigDecimal> {

  @Override
  public Class<BigDecimal> getConvertedType() {
    return BigDecimal.class;
  }

  @Override
  public String getLogicalTypeName() {
    return BigDecimalLogicalType.INSTANCE.getName();
  }

  @Override
  public Schema getRecommendedSchema() {
    return BigDecimalLogicalType.INSTANCE.addToSchema(Schema.create(Schema.Type.STRING));
  }

  @Override
  public CharSequence toCharSequence(BigDecimal value, Schema schema, LogicalType type) {
    return value.toString();
  }

  @Override
  public BigDecimal fromCharSequence(CharSequence value, Schema schema, LogicalType type) {
    return new BigDecimal(value.toString());
  }
}
