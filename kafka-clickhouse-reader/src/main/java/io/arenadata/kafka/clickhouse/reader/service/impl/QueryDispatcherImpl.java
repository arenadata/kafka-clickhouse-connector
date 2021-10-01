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
package io.arenadata.kafka.clickhouse.reader.service.impl;

import io.arenadata.kafka.clickhouse.reader.model.QueryRequest;
import io.arenadata.kafka.clickhouse.reader.service.QueryDispatcher;
import io.vertx.core.Future;
import org.springframework.stereotype.Component;

@Component
public class QueryDispatcherImpl implements QueryDispatcher {

    private BaseQueryExecutorService queryExecutorService;

    public QueryDispatcherImpl(ClickhouseQueryExecutorService queryExecutorService) {
        this.queryExecutorService = queryExecutorService;
    }

    @Override
    public Future<Void> dispatch(QueryRequest query) {
        return Future.future(p -> queryExecutorService.execute(query, p));
    }
}
