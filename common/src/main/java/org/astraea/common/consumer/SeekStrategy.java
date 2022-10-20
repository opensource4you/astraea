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
package org.astraea.common.consumer;

import static java.util.Objects.requireNonNull;

import java.util.Map;
import java.util.function.BiConsumer;
import org.apache.kafka.clients.consumer.Consumer;
import org.astraea.common.EnumInfo;
import org.astraea.common.admin.TopicPartition;

public enum SeekStrategy implements EnumInfo {
  NONE((consumer, seekValue) -> {}),
  DISTANCE_FROM_LATEST(
      (kafkaConsumer, distanceFromLatest) -> {
        // this mode is not supported by kafka, so we have to calculate the offset first
        var partitions = kafkaConsumer.assignment();
        // 1) get the end offsets from all subscribed partitions
        var endOffsets = kafkaConsumer.endOffsets(partitions);
        // 2) calculate and then seek to the correct offset (end offset - recent offset)
        endOffsets.forEach(
            (tp, latest) ->
                kafkaConsumer.seek(tp, Math.max(0, latest - (long) distanceFromLatest)));
      }),
  DISTANCE_FROM_BEGINNING(
      (kafkaConsumer, distanceFromBeginning) -> {
        var partitions = kafkaConsumer.assignment();
        var beginningOffsets = kafkaConsumer.beginningOffsets(partitions);
        beginningOffsets.forEach(
            (tp, beginning) -> kafkaConsumer.seek(tp, beginning + (long) distanceFromBeginning));
      }),
  @SuppressWarnings("unchecked")
  SEEK_TO(
      (kafkaConsumer, seekTo) -> {
        if (seekTo instanceof Long) {
          var partitions = kafkaConsumer.assignment();
          partitions.forEach(tp -> kafkaConsumer.seek(tp, (long) seekTo));
          return;
        }
        if (seekTo instanceof Map) {
          var partitions = kafkaConsumer.assignment();
          ((Map<TopicPartition, Long>) seekTo)
              .entrySet().stream()
                  // don't seek the partition which is not belonged to this consumer
                  .filter(e -> partitions.contains(TopicPartition.to(e.getKey())))
                  .forEach(e -> kafkaConsumer.seek(TopicPartition.to(e.getKey()), e.getValue()));
          return;
        }
        throw new IllegalArgumentException(
            seekTo.getClass().getSimpleName() + " is not correct type");
      });

  public static SeekStrategy ofAlias(String alias) {
    return EnumInfo.ignoreCaseEnum(SeekStrategy.class, alias);
  }

  @Override
  public String alias() {
    return name();
  }

  @Override
  public String toString() {
    return alias();
  }

  private final BiConsumer<Consumer<?, ?>, Object> function;

  SeekStrategy(BiConsumer<org.apache.kafka.clients.consumer.Consumer<?, ?>, Object> function) {
    this.function = requireNonNull(function);
  }

  void apply(org.apache.kafka.clients.consumer.Consumer<?, ?> kafkaConsumer, Object seekValue) {
    if (seekValue != null) function.accept(kafkaConsumer, seekValue);
  }
}
