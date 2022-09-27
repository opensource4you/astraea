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
import org.apache.kafka.common.errors.WakeupException;
import org.astraea.common.Utils;
import org.astraea.common.consumer.ConsumerRebalanceListener;
import org.astraea.common.consumer.SubscribedConsumer;

public interface ConsumerThread extends AbstractThread {
  static List<ConsumerThread> create(
      int consumers,
      Function<ConsumerRebalanceListener, SubscribedConsumer<byte[], byte[]>> consumerSupplier,
      Supplier<Performance.RecordListener> recordListener) {
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
              @SuppressWarnings("resource")
              var listener = recordListener.get();
              var consumer = consumerSupplier.apply(listener);
              var closed = new AtomicBoolean(false);
              var closeLatch = closeLatches.get(index);
              var subscribed = new AtomicBoolean(true);
              Performance.RecordListener.stickyNumbers.putIfAbsent(consumer.clientId(), 0);
              listener.clientId(consumer.clientId());
              executors.execute(
                  () -> {
                    try {
                      while (!closed.get()) {
                        if (subscribed.get()) consumer.resubscribe();
                        else {
                          consumer.unsubscribe();
                          Utils.sleep(Duration.ofSeconds(1));
                          Performance.RecordListener.stickyNumbers.put(consumer.clientId(), 0);
                          listener.flushPrevPartitions();
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
}
