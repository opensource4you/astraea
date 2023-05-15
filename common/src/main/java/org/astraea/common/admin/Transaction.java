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

import java.util.Set;
import java.util.stream.Collectors;

public record Transaction(
    String transactionId,
    int coordinatorId,
    TransactionState state,
    long producerId,
    int producerEpoch,
    long transactionTimeoutMs,
    Set<TopicPartition> topicPartitions) {

  static Transaction of(
      String transactionId, org.apache.kafka.clients.admin.TransactionDescription td) {
    return new Transaction(
        transactionId,
        td.coordinatorId(),
        TransactionState.of(td.state()),
        td.producerId(),
        td.producerEpoch(),
        td.transactionTimeoutMs(),
        td.topicPartitions().stream().map(TopicPartition::from).collect(Collectors.toSet()));
  }
}
