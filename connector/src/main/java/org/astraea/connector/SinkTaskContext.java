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
package org.astraea.connector;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import org.astraea.common.admin.TopicPartition;

public class SinkTaskContext implements TaskContext {

  private final org.apache.kafka.connect.sink.SinkTaskContext context;

  public SinkTaskContext(org.apache.kafka.connect.sink.SinkTaskContext context) {
    this.context = context;
  }

  @Override
  public void offset(Map<TopicPartition, Long> offsets) {
    this.context.offset(
        offsets.entrySet().stream()
            .collect(Collectors.toMap(e -> TopicPartition.to(e.getKey()), Map.Entry::getValue)));
  }

  @Override
  public void offset(TopicPartition topicPartition, long offset) {
    this.context.offset(TopicPartition.to(topicPartition), offset);
  }

  @Override
  public void pause(TopicPartition... partitions) {
    this.context.pause(
        Arrays.stream(partitions)
            .map(TopicPartition::to)
            .toArray(org.apache.kafka.common.TopicPartition[]::new));
  }

  @Override
  public void requestCommit() {
    this.context.requestCommit();
  }
}
