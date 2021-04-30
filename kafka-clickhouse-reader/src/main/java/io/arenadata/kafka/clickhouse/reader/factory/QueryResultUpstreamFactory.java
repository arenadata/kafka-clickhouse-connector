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
package io.arenadata.kafka.clickhouse.reader.factory;

import io.arenadata.kafka.clickhouse.reader.model.QueryResultItem;
import io.arenadata.kafka.clickhouse.reader.service.PublishService;
import io.arenadata.kafka.clickhouse.reader.upstream.QueryResultUpstream;
import io.arenadata.kafka.clickhouse.reader.upstream.Upstream;
import org.apache.avro.Schema;

public class QueryResultUpstreamFactory implements UpstreamFactory<QueryResultItem> {

    private final PublishService publishService;

    public QueryResultUpstreamFactory(PublishService publishService) {
        this.publishService = publishService;
    }

    @Override
    public Upstream<QueryResultItem> create(String avroSchema) {
        return new QueryResultUpstream(publishService, new Schema.Parser().parse(avroSchema));
    }

    @Override
    public void close() {

    }
}
