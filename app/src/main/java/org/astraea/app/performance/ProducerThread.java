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
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.common.Utils;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.partitioner.Dispatcher;
import org.astraea.common.producer.Producer;
import org.astraea.common.producer.Record;

public interface ProducerThread extends AbstractThread {
  DataSupplier NO_AVAILABLE_DATA_SUPPLIER = null;

  static List<ProducerThread> create(
      int batchSize,
      Function<TopicPartition, DataSupplier> dataSupplierFunction,
      Supplier<TopicPartition> topicPartitionSupplier,
      int producers,
      Supplier<Producer<byte[], byte[]>> producerSupplier,
      int interdependent) {
    if (producers <= 0) return List.of();
    var closeLatches =
        IntStream.range(0, producers)
            .mapToObj(ignored -> new CountDownLatch(1))
            .collect(Collectors.toUnmodifiableList());
    var executors = Executors.newFixedThreadPool(producers);
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
    return IntStream.range(0, producers)
        .mapToObj(
            index -> {
              var closeLatch = closeLatches.get(index);
              var closed = new AtomicBoolean(false);
              var producer = producerSupplier.get();
              executors.execute(
                  () -> {
                    try {
                      int interdependentCounter = 0;
                      while (!closed.get()) {
                        var partition = topicPartitionSupplier.get();
                        var dataSupplier = dataSupplierFunction.apply(partition);
                        if (dataSupplier == NO_AVAILABLE_DATA_SUPPLIER || !dataSupplier.active())
                          continue;

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
                          dataSupplier.setActive(true);
                          continue;
                        }

                        // Using interdependent
                        if (interdependent > 1) {
                          Dispatcher.beginInterdependent(producer);
                          interdependentCounter +=
                              data.stream().filter(DataSupplier.Data::hasData).count();
                        }
                        producer.send(
                            data.stream()
                                .filter(DataSupplier.Data::hasData)
                                .map(
                                    d ->
                                        Record.builder()
                                            .topicPartition(partition)
                                            .key(d.key())
                                            .value(d.value())
                                            .timestamp(System.currentTimeMillis())
                                            .build())
                                .collect(Collectors.toList()));
                        // End interdependent
                        if (interdependent > 1 && interdependentCounter >= interdependent) {
                          Dispatcher.endInterdependent(producer);
                          interdependentCounter = 0;
                        }
                      }
                    } finally {
                      Utils.swallowException(producer::close);
                      closeLatch.countDown();
                      closed.set(true);
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
                public void close() {
                  closed.set(true);
                  waitForDone();
                }
              };
            })
        .collect(Collectors.toUnmodifiableList());
  }
}
