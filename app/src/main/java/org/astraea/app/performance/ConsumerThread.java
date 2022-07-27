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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.common.errors.WakeupException;
import org.astraea.app.common.Utils;
import org.astraea.app.consumer.Consumer;

public interface ConsumerThread extends AbstractThread {

  static List<ConsumerThread> create(List<Consumer<byte[], byte[]>> consumers) {
    if (consumers.isEmpty()) return List.of();
    var reports =
        IntStream.range(0, consumers.size())
            .mapToObj(ignored -> new Report())
            .collect(Collectors.toUnmodifiableList());
    var closeLatches =
        IntStream.range(0, consumers.size())
            .mapToObj(ignored -> new CountDownLatch(1))
            .collect(Collectors.toUnmodifiableList());
    var closeFlags =
        IntStream.range(0, consumers.size())
            .mapToObj(ignored -> new AtomicBoolean(false))
            .collect(Collectors.toUnmodifiableList());
    var executors = Executors.newFixedThreadPool(consumers.size());
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
    return IntStream.range(0, consumers.size())
        .mapToObj(
            index -> {
              var consumer = consumers.get(index);
              var report = reports.get(index);
              var closeLatch = closeLatches.get(index);
              var closed = closeFlags.get(index);
              executors.execute(
                  () -> {
                    try {
                      while (!closed.get()) {
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
                      }
                    } catch (WakeupException ignore) {
                      // Stop polling and being ready to clean up
                    } finally {
                      try {
                        consumer.close();
                      } finally {
                        closeLatch.countDown();
                      }
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
                public Report report() {
                  return report;
                }

                @Override
                public void close() {
                  closed.set(true);
                  consumer.wakeup();
                  Utils.swallowException(closeLatch::await);
                }
              };
            })
        .collect(Collectors.toUnmodifiableList());
  }

  /** @return report of this thread */
  Report report();
}
