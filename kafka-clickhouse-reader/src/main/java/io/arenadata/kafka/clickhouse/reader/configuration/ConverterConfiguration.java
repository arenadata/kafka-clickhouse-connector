/*
 * Copyright © 2021 Arenadata Software LLC
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
package io.arenadata.kafka.clickhouse.reader.configuration;

import io.arenadata.kafka.clickhouse.reader.converter.impl.*;
import io.arenadata.kafka.clickhouse.reader.converter.transformer.ColumnTransformer;
import org.apache.avro.Schema;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

import static io.arenadata.kafka.clickhouse.reader.converter.transformer.ColumnTransformer.getTransformerMap;

@Configuration
public class ConverterConfiguration {

    @Bean("clickhouseTransformerMap")
    public Map<Schema.Type, Map<Class<?>, ColumnTransformer>> clickhouseTransformerMap() {
        Map<Schema.Type, Map<Class<?>, ColumnTransformer>> transformerMap = new HashMap<>();
        transformerMap.put(Schema.Type.STRING, getTransformerMap(new StringFromStringTransformer(), new StringFromObjectTransformer()));
        transformerMap.put(Schema.Type.INT, getTransformerMap(new IntFromNumberTransformer(), new IntFromLocalDateTransformer()));
        transformerMap.put(Schema.Type.LONG, getTransformerMap(new LongFromNumberTransformer(), new LongFromLocalDateTimeTransformer(), new LongFromLocalTimeTransformer()));
        transformerMap.put(Schema.Type.FLOAT, getTransformerMap(new FloatFromNumberTransformer()));
        transformerMap.put(Schema.Type.DOUBLE, getTransformerMap(new DoubleFromNumberTransformer()));
        transformerMap.put(Schema.Type.BOOLEAN, getTransformerMap(new BooleanFromBooleanTransformer(), new BooleanFromNumberTransformer()));
        return transformerMap;
    }
}
