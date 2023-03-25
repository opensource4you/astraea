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
package org.astraea.app.publisher;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.metrics.collector.MetricsFetcher;
import org.astraea.common.producer.Producer;
import org.astraea.common.producer.Record;
import org.astraea.common.producer.Serializer;

public class TopicSender implements MetricsFetcher.Sender {
  private final Producer<byte[], String> producer;

  public TopicSender(String bootstrapServer) {
    producer =
        Producer.builder()
            .bootstrapServers(bootstrapServer)
            .valueSerializer(Serializer.STRING)
            .build();
  }

  @Override
  public CompletionStage<Void> send(int id, Collection<BeanObject> beans) {
    var records =
        beans.stream()
            .map(
                bean ->
                    Record.builder()
                        .topic(MetricPublisher.internalTopicName(Integer.toString(id)))
                        .key((byte[]) null)
                        .value(bean.toString())
                        .build())
            .collect(Collectors.toUnmodifiableList());
    return producer.send(records).stream()
        .reduce(
            CompletableFuture.completedStage(null),
            (stage1, stage2) -> stage1.thenCombine(stage2, (ignore1, ignore2) -> null),
            (stage1, stage2) -> null);
  }

  @Override
  public void close() {
    producer.close();
  }
}
