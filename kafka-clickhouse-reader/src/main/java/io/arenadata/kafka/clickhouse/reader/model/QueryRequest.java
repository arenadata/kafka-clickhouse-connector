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
package io.arenadata.kafka.clickhouse.reader.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class QueryRequest {
    String table;
    String datamart;
    String sql;
    List<KafkaBrokerInfo> kafkaBrokers;
    String kafkaTopic;
    int chunkSize = 1000;
    String avroSchema;
    int streamNumber = 0;
    int streamTotal = 1;

    public QueryRequest copy() {
        final QueryRequest newQueryRequest = new QueryRequest();
        newQueryRequest.setTable(table);
        newQueryRequest.setDatamart(datamart);
        newQueryRequest.setSql(sql);
        newQueryRequest.setKafkaTopic(kafkaTopic);
        newQueryRequest.setKafkaBrokers(kafkaBrokers);
        newQueryRequest.setChunkSize(chunkSize);
        newQueryRequest.setAvroSchema(avroSchema);
        newQueryRequest.setStreamNumber(streamNumber);
        newQueryRequest.setStreamTotal(streamTotal);
        return newQueryRequest;
    }
}
