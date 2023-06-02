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
package org.astraea.common.admin;

import java.util.List;
import java.util.Optional;

/**
 * @param earliestOffset existent earliest offset
 * @param latestOffset existent latest offset
 * @param maxTimestamp max timestamp of existent records. If the kafka servers don't support to
 *     fetch max timestamp, this method will return empty
 * @param leaderId null if the node gets offline. otherwise, it returns node id.
 * @param internal true if this topic is internal (system) topic
 */
public record Partition(
    String topic,
    int partition,
    long earliestOffset,
    long latestOffset,
    Optional<Long> maxTimestamp,
    Optional<Integer> leaderId,
    List<Broker> replicas,
    List<Broker> isr,
    boolean internal) {

  public TopicPartition topicPartition() {
    return TopicPartition.of(topic(), partition());
  }
}
