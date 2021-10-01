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
package io.arenadata.kafka.clickhouse.writer.service.kafka;

import io.arenadata.kafka.clickhouse.writer.model.InsertDataContext;
import io.arenadata.kafka.clickhouse.writer.model.kafka.TopicPartitionConsumer;
import io.vertx.core.Future;
import io.vertx.kafka.client.common.PartitionInfo;
import io.vertx.kafka.client.consumer.KafkaConsumer;

import java.util.List;

public interface KafkaConsumerService {
    Future<List<PartitionInfo>> getTopicPartitions(InsertDataContext context);

    KafkaConsumer<byte[], byte[]> createConsumer(InsertDataContext context);

    Future<TopicPartitionConsumer> createTopicPartitionConsumer(InsertDataContext context, PartitionInfo partitionInfo);
}
