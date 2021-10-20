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
package io.arenadata.kafka.clickhouse.reader.factory;

import io.arenadata.kafka.clickhouse.reader.model.KafkaBrokerInfo;
import io.arenadata.kafka.clickhouse.reader.model.QueryResultItem;
import io.arenadata.kafka.clickhouse.reader.service.KafkaProducerProvider;
import io.arenadata.kafka.clickhouse.reader.service.PublishService;
import io.arenadata.kafka.clickhouse.reader.upstream.QueryResultUpstream;
import io.arenadata.kafka.clickhouse.reader.upstream.Upstream;
import lombok.val;
import org.apache.avro.Schema;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service
public class QueryResultUpstreamFactory implements UpstreamFactory<QueryResultItem> {

    private final PublishService publishService;
    private final KafkaProducerProvider kafkaProducerProvider;

    public QueryResultUpstreamFactory(PublishService publishService,
                                      KafkaProducerProvider kafkaProducerProvider) {
        this.publishService = publishService;
        this.kafkaProducerProvider = kafkaProducerProvider;
    }

    @Override
    public Upstream<QueryResultItem> create(String avroSchema, List<KafkaBrokerInfo> kafkaBrokers) {
        val kafkaBrokersListStr = kafkaBrokers.stream().map(KafkaBrokerInfo::getAddress)
                .collect(Collectors.joining(","));
        val kafkaProducer = kafkaProducerProvider.create(kafkaBrokersListStr);

        return new QueryResultUpstream(publishService, new Schema.Parser().parse(avroSchema), kafkaProducer);
    }

    @Override
    public void close() {

    }
}
