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
package org.astraea.app.performance;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.common.errors.WakeupException;
import org.astraea.common.Utils;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.consumer.ConsumerRebalanceListener;
import org.astraea.common.consumer.SubscribedConsumer;

public interface ConsumerThread extends AbstractThread {
  ConcurrentMap<String, Set<TopicPartition>> CLIENT_ID_ASSIGNED_PARTITIONS =
      new ConcurrentHashMap<>();
  ConcurrentMap<String, Set<TopicPartition>> CLIENT_ID_REVOKED_PARTITIONS =
      new ConcurrentHashMap<>();

  ConcurrentMap<String, Integer> CLIENT_ID_NUM_PARTITIONS = new ConcurrentHashMap<>();

  static long nonStickyPartitionBetweenRebalance(String clientId) {
    return CLIENT_ID_ASSIGNED_PARTITIONS.getOrDefault(clientId, Set.of()).stream()
        .dropWhile(tp -> CLIENT_ID_REVOKED_PARTITIONS.getOrDefault(clientId, Set.of()).contains(tp))
        .count();
  }

  static long differenceBetweenRebalance(String clientId) {
    return CLIENT_ID_ASSIGNED_PARTITIONS.getOrDefault(clientId, Set.of()).size()
        - CLIENT_ID_REVOKED_PARTITIONS.getOrDefault(clientId, Set.of()).size();
  }

  static long numOfPartitions(String clientId) {
    return CLIENT_ID_NUM_PARTITIONS.getOrDefault(clientId, 0);
  }

  static List<ConsumerThread> create(
      int consumers,
      BiFunction<String, ConsumerRebalanceListener, SubscribedConsumer<byte[], byte[]>>
          consumerSupplier) {
    if (consumers == 0) return List.of();
    var closeLatches =
        IntStream.range(0, consumers)
            .mapToObj(ignored -> new CountDownLatch(1))
            .collect(Collectors.toUnmodifiableList());
    var executors = Executors.newFixedThreadPool(consumers);
    // monitor
    CompletableFuture.runAsync(
        () -> {
          try {
            closeLatches.forEach(l -> Utils.swallowException(l::await));
          } finally {
            executors.shutdown();
            Utils.swallowException(() -> executors.awaitTermination(30, TimeUnit.SECONDS));
          }
        });
    return IntStream.range(0, consumers)
        .mapToObj(
            index -> {
              var clientId = Utils.randomString();
              var consumer = consumerSupplier.apply(clientId, new PartitionRatioListener(clientId));
              var closed = new AtomicBoolean(false);
              var closeLatch = closeLatches.get(index);
              var subscribed = new AtomicBoolean(true);
              executors.execute(
                  () -> {
                    try {
                      while (!closed.get()) {
                        if (subscribed.get()) consumer.resubscribe();
                        else {
                          consumer.unsubscribe();
                          Utils.sleep(Duration.ofSeconds(1));
                          continue;
                        }
                        consumer.poll(Duration.ofSeconds(1));
                      }
                    } catch (WakeupException ignore) {
                      // Stop polling and being ready to clean up
                    } finally {
                      Utils.swallowException(consumer::close);
                      closeLatch.countDown();
                      closed.set(true);
                      CLIENT_ID_ASSIGNED_PARTITIONS.remove(clientId);
                      CLIENT_ID_REVOKED_PARTITIONS.remove(clientId);
                    }
                  });
              return new ConsumerThread() {

                @Override
                public void waitForDone() {
                  Utils.swallowException(closeLatch::await);
                }

                @Override
                public boolean closed() {
                  return closeLatch.getCount() == 0;
                }

                @Override
                public void resubscribe() {
                  subscribed.set(true);
                }

                @Override
                public void unsubscribe() {
                  subscribed.set(false);
                }

                @Override
                public void close() {
                  closed.set(true);
                  Utils.swallowException(closeLatch::await);
                }
              };
            })
        .collect(Collectors.toUnmodifiableList());
  }

  void resubscribe();

  void unsubscribe();

  class PartitionRatioListener implements ConsumerRebalanceListener {
    private final String clientId;

    PartitionRatioListener(String clientId) {
      this.clientId = clientId;
    }

    @Override
    public void onPartitionAssigned(Set<TopicPartition> partitions) {
      CLIENT_ID_ASSIGNED_PARTITIONS.put(clientId, partitions);
      CLIENT_ID_NUM_PARTITIONS.compute(
          clientId, (id, old) -> ((old == null) ? 0 : old) + partitions.size());
    }

    @Override
    public void onPartitionsRevoked(Set<TopicPartition> partitions) {
      CLIENT_ID_REVOKED_PARTITIONS.put(clientId, partitions);
      CLIENT_ID_NUM_PARTITIONS.compute(
          clientId, (id, old) -> ((old == null) ? 0 : old) - partitions.size());
    }
  }
}
