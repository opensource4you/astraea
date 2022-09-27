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
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.ListOffsetsResult;

public interface Partition {

  static Partition of(
      String topic,
      org.apache.kafka.common.TopicPartitionInfo tpi,
      Optional<org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo> earliest,
      Optional<org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo> latest) {
    return of(
        topic,
        tpi.partition(),
        NodeInfo.of(tpi.leader()),
        tpi.replicas().stream().map(NodeInfo::of).collect(Collectors.toList()),
        tpi.isr().stream().map(NodeInfo::of).collect(Collectors.toList()),
        earliest.map(ListOffsetsResult.ListOffsetsResultInfo::offset).orElse(-1L),
        earliest.map(ListOffsetsResult.ListOffsetsResultInfo::timestamp).orElse(-1L),
        latest.map(ListOffsetsResult.ListOffsetsResultInfo::offset).orElse(-1L),
        latest.map(ListOffsetsResult.ListOffsetsResultInfo::timestamp).orElse(-1L));
  }

  static Partition of(
      String topic,
      int partition,
      NodeInfo leader,
      List<NodeInfo> replicas,
      List<NodeInfo> isr,
      long earliestOffset,
      long earliestOffsetTimestamp,
      long latestOffset,
      long latestOffsetTimestamp) {
    return new Partition() {

      @Override
      public String topic() {
        return topic;
      }

      @Override
      public int partition() {
        return partition;
      }

      @Override
      public long earliestOffset() {
        return earliestOffset;
      }

      @Override
      public long earliestOffsetTimestamp() {
        return earliestOffsetTimestamp;
      }

      @Override
      public long latestOffset() {
        return latestOffset;
      }

      @Override
      public long latestOffsetTimestamp() {
        return latestOffsetTimestamp;
      }

      @Override
      public NodeInfo leader() {
        return leader;
      }

      @Override
      public List<NodeInfo> replicas() {
        return replicas;
      }

      @Override
      public List<NodeInfo> isr() {
        return isr;
      }
    };
  }

  default TopicPartition topicPartition() {
    return TopicPartition.of(topic(), partition());
  }

  String topic();

  int partition();

  /** @return existent earliest offset */
  long earliestOffset();

  /** @return timestamp of the earliest offset */
  long earliestOffsetTimestamp();

  /** @return existent latest offset */
  long latestOffset();

  /** @return timestamp of the latest offset */
  long latestOffsetTimestamp();

  NodeInfo leader();

  List<NodeInfo> replicas();

  List<NodeInfo> isr();
}
