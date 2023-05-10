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

public record Partition(
    String topic,
    int partition,
    // existent earliest offset
    long earliestOffset,
    // existent latest offset
    long latestOffset,
    // max timestamp of existent records. If the kafka servers don't support to fetch max
    // timestamp, this method will return empty
    Optional<Long> maxTimestamp,
    // null if the node gets offline. otherwise, it returns node info.
    Optional<NodeInfo> leader,
    List<NodeInfo> replicas,
    List<NodeInfo> isr,
    // true if this topic is internal (system) topic
    boolean internal) {

  public TopicPartition topicPartition() {
    return TopicPartition.of(topic(), partition());
  }
}
