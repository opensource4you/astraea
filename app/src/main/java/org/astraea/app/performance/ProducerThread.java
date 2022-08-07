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
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.app.common.Utils;
import org.astraea.app.producer.Producer;

public interface ProducerThread extends AbstractThread {

  static List<ProducerThread> create(
      String topic,
      int batchSize,
      DataSupplier dataSupplier,
      Supplier<Integer> partitionSupplier,
      List<Producer<byte[], byte[]>> producers) {
    if (producers.isEmpty()) return List.of();
    var reports =
        IntStream.range(0, producers.size())
            .mapToObj(ignored -> new Report())
            .collect(Collectors.toUnmodifiableList());
    var closeLatches =
        IntStream.range(0, producers.size())
            .mapToObj(ignored -> new CountDownLatch(1))
            .collect(Collectors.toUnmodifiableList());
    var closedFlags =
        IntStream.range(0, producers.size())
            .mapToObj(ignored -> new AtomicBoolean(false))
            .collect(Collectors.toUnmodifiableList());
    var executors = Executors.newFixedThreadPool(producers.size());
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
    return IntStream.range(0, producers.size())
        .mapToObj(
            index -> {
              var producer = producers.get(index);
              var report = reports.get(index);
              var closeLatch = closeLatches.get(index);
              var closed = closedFlags.get(index);
              executors.execute(
                  () -> {
                    try {
                      while (!closed.get()) {
                        var data =
                            IntStream.range(0, batchSize)
                                .mapToObj(i -> dataSupplier.get())
                                .collect(Collectors.toUnmodifiableList());

                        // no more data
                        if (data.stream().allMatch(DataSupplier.Data::done)) return;

                        // no data due to throttle
                        // TODO: we should return a precise sleep time
                        if (data.stream().allMatch(DataSupplier.Data::throttled)) {
                          Utils.sleep(Duration.ofSeconds(1));
                          continue;
                        }
                        producer
                            .send(
                                data.stream()
                                    .filter(DataSupplier.Data::hasData)
                                    .map(
                                        d ->
                                            producer
                                                .sender()
                                                .topic(topic)
                                                .partition(partitionSupplier.get())
                                                .key(d.key())
                                                .value(d.value())
                                                .timestamp(System.currentTimeMillis()))
                                    .collect(Collectors.toList()))
                            .forEach(
                                future ->
                                    future.whenComplete(
                                        (m, e) ->
                                            report.record(
                                                m.topic(),
                                                m.partition(),
                                                m.offset(),
                                                System.currentTimeMillis() - m.timestamp(),
                                                m.serializedValueSize() + m.serializedKeySize())));
                      }
                    } finally {
                      try {
                        producer.close();
                      } finally {
                        closeLatch.countDown();
                      }
                    }
                  });
              return new ProducerThread() {

                @Override
                public boolean closed() {
                  return closeLatch.getCount() == 0;
                }

                @Override
                public void waitForDone() {
                  Utils.swallowException(closeLatch::await);
                }

                @Override
                public Report report() {
                  return report;
                }

                @Override
                public void close() {
                  closed.set(true);
                  waitForDone();
                }
              };
            })
        .collect(Collectors.toUnmodifiableList());
  }

  /** @return report of this thread */
  Report report();
}
