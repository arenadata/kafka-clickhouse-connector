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
package io.arenadata.kafka.clickhouse.reader.converter.impl;

import io.arenadata.kafka.clickhouse.reader.converter.transformer.AbstractColumnTransformer;
import io.arenadata.kafka.clickhouse.util.DateTimeUtils;

import java.time.LocalDate;
import java.util.Collection;
import java.util.Collections;

public class IntFromLocalDateTransformer extends AbstractColumnTransformer<Integer, LocalDate> {

    @Override
    public Integer transformValue(LocalDate value) {
        return value != null ? DateTimeUtils.toEpochDay(value).intValue() : null;
    }

    @Override
    public Collection<Class<?>> getTransformClasses() {
        return Collections.singletonList(LocalDate.class);
    }
}
