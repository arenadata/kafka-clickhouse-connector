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
package io.arenadata.kafka.clickhouse.reader.service;

import io.arenadata.kafka.clickhouse.reader.model.DtmQueryResponseMetadata;
import io.arenadata.kafka.clickhouse.reader.model.KafkaBrokerInfo;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

import java.util.List;

public interface PublishService {
    void publishQueryResult(List<KafkaBrokerInfo> kafkaBrokers,
                            String topicName,
                            DtmQueryResponseMetadata key,
                            byte[] message,
                            Handler<AsyncResult<Void>> handler);
}
