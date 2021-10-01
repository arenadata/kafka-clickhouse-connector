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

import io.arenadata.kafka.clickhouse.reader.factory.UpstreamFactory;
import io.arenadata.kafka.clickhouse.reader.model.QueryResultItem;
import io.arenadata.kafka.clickhouse.reader.service.DatabaseExecutor;
import io.arenadata.kafka.clickhouse.reader.verticle.TaskVerticleExecutor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Slf4j
public class ClickhouseQueryExecutorService extends BaseQueryExecutorService {

    @Autowired
    public ClickhouseQueryExecutorService(@Qualifier("clickhouseDatabaseExecutorList") List<DatabaseExecutor> databaseExecutors,
                                          UpstreamFactory<QueryResultItem> upstreamFactory,
                                          TaskVerticleExecutor taskExecutor) {
        super(databaseExecutors, upstreamFactory, taskExecutor);
    }
}
