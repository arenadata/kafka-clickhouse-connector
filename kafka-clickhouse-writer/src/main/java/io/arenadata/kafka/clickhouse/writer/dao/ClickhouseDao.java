/*
 * Copyright © 2021 Kafka Clickhouse Writer
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
package io.arenadata.kafka.clickhouse.writer.dao;

import io.arenadata.kafka.clickhouse.writer.model.sql.InsertSqlRequest;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

public interface ClickhouseDao {

    void insertRows(InsertSqlRequest request, Handler<AsyncResult<Integer>> handler);
}
