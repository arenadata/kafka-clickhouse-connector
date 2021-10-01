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
package io.arenadata.kafka.clickhouse.writer.repository.impl;

import io.arenadata.kafka.clickhouse.writer.model.InsertDataContext;
import io.arenadata.kafka.clickhouse.writer.repository.InsertDataContextRepository;
import org.springframework.stereotype.Repository;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Repository
public class InsertDataContextRepositoryImpl implements InsertDataContextRepository {
  private final Map<String, InsertDataContext> map;

  public InsertDataContextRepositoryImpl() {
    map = new ConcurrentHashMap<>();
  }

  @Override
  public Optional<InsertDataContext> get(String contextId) {
    return Optional.ofNullable(map.get(contextId));
  }

  @Override
  public void add(InsertDataContext context) {
    map.put(context.getContextId(), context);
  }

  @Override
  public Optional<InsertDataContext> remove(String contextId) {
    return Optional.ofNullable(map.remove(contextId));
  }
}
