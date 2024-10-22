/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.astraea.common.metrics.collector;

import java.util.Collection;
import java.util.concurrent.CompletionStage;
import org.astraea.common.FutureUtils;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.producer.Producer;
import org.astraea.common.producer.ProducerConfigs;
import org.astraea.common.producer.Record;
import org.astraea.common.producer.Serializer;

public interface MetricSender extends AutoCloseable {

  static MetricSender local() {
    return LocalSenderReceiver.of();
  }

  static MetricSender topic(String bootstrapServer) {
    var producer =
        Producer.builder()
            .bootstrapServers(bootstrapServer)
            .keySerializer(Serializer.INTEGER)
            .valueSerializer(Serializer.BEAN_OBJECT)
            .config(ProducerConfigs.ACKS_CONFIG, "0")
            .config(ProducerConfigs.ENABLE_METRICS_PUSH_CONFIG, "false")
            .config(ProducerConfigs.COMPRESSION_TYPE_CONFIG, "gzip")
            .build();
    String METRIC_TOPIC = "__metrics";
    return new MetricSender() {
      @Override
      public CompletionStage<Void> send(int id, Collection<BeanObject> beans) {
        var records =
            beans.stream()
                .map(bean -> Record.builder().topic(METRIC_TOPIC).key(id).value(bean).build())
                .toList();
        return FutureUtils.sequence(
                producer.send(records).stream().map(CompletionStage::toCompletableFuture).toList())
            .thenAccept(ignored -> {});
      }

      @Override
      public void close() {
        producer.close();
      }
    };
  }

  CompletionStage<Void> send(int id, Collection<BeanObject> beans);

  @Override
  default void close() {}
}
