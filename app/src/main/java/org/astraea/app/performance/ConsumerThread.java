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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.common.errors.WakeupException;
import org.astraea.app.admin.TopicPartition;
import org.astraea.app.common.Utils;
import org.astraea.app.consumer.ConsumerRebalanceListener;
import org.astraea.app.consumer.SubscribedConsumer;

public interface ConsumerThread extends AbstractThread {

  static List<ConsumerThread> create(
      int consumers,
      Function<ConsumerRebalanceListener, SubscribedConsumer<byte[], byte[]>> consumerSupplier) {
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
              var report = new Report();
              var listener = new Listener(report);
              var closeLatch = closeLatches.get(index);
              var closed = new AtomicBoolean(false);
              var subscribed = new AtomicBoolean(true);
              executors.execute(
                  () -> {
                    try (var consumer = consumerSupplier.apply(listener)) {
                      while (!closed.get()) {
                        if (subscribed.get()) consumer.resubscribe();
                        else {
                          consumer.unsubscribe();
                          report.assignments(Set.of());
                          Utils.sleep(Duration.ofSeconds(1));
                          continue;
                        }
                        consumer
                            .poll(Duration.ofSeconds(1))
                            .forEach(
                                record ->
                                    // record ene-to-end latency, and record input byte (header and
                                    // timestamp size excluded)
                                    report.record(
                                        record.topic(),
                                        record.partition(),
                                        record.offset(),
                                        System.currentTimeMillis() - record.timestamp(),
                                        record.serializedKeySize() + record.serializedValueSize()));
                        report.assignments(consumer.assignments());
                      }
                    } catch (WakeupException ignore) {
                      // Stop polling and being ready to clean up
                    } finally {
                      closeLatch.countDown();
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
                public Report report() {
                  return report;
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

  /** @return report of this thread */
  Report report();

  class Listener implements ConsumerRebalanceListener {
    private final Report report;
    private long previousCall = System.currentTimeMillis();
    private long maxLatency = 0;
    private long sumLatency = 0;
    private long count = 0;

    public Listener(Report report) {
      this.report = report;
    }

    @Override
    public void onPartitionAssigned(Set<TopicPartition> partitions) {
      record();
    }

    @Override
    public void onPartitionsRevoked(Set<TopicPartition> partitions) {
      record();
    }

    private void record() {
      count += 1;
      var current = System.currentTimeMillis();
      var diff = current - previousCall;
      maxLatency = Math.max(maxLatency, diff);
      sumLatency += diff;
      previousCall = current;
      report.maxSubscriptionLatency(maxLatency);
      report.avgSubscriptionLatency((double) sumLatency / count);
    }
  }

  class Report extends org.astraea.app.performance.Report.Impl {
    private volatile long maxSubscriptionLatency = 0;
    private volatile double avgSubscriptionLatency = 0;

    private volatile Set<TopicPartition> assignments;

    public long maxSubscriptionLatency() {
      return maxSubscriptionLatency;
    }

    public void maxSubscriptionLatency(long maxSubscriptionLatency) {
      this.maxSubscriptionLatency = maxSubscriptionLatency;
    }

    public double avgSubscriptionLatency() {
      return avgSubscriptionLatency;
    }

    public void avgSubscriptionLatency(double avgSubscriptionLatency) {
      this.avgSubscriptionLatency = avgSubscriptionLatency;
    }

    public void assignments(Set<TopicPartition> assignments) {
      this.assignments = assignments;
    }

    public Set<TopicPartition> assignments() {
      return assignments;
    }
  }
}
