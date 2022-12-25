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

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;
import org.astraea.common.Utils;
import org.astraea.common.admin.TopicPartition;

public interface DataGenerator extends AbstractThread {
  static DataGenerator of(
      BlockingQueue<List<DataSupplier.Data>> queue,
      Supplier<TopicPartition> partitionSelector,
      Function<TopicPartition, List<DataSupplier.Data>> dataSupplier) {
    var closeLatch = new CountDownLatch(1);
    var executor = Executors.newFixedThreadPool(1);
    var closed = new AtomicBoolean(false);
    // monitor the data generator if close or not
    CompletableFuture.runAsync(
        () -> {
          try {
            Utils.swallowException(closeLatch::await);
          } finally {
            executor.shutdown();
            Utils.swallowException(() -> executor.awaitTermination(30, TimeUnit.SECONDS));
          }
        });
    // put the data into blocking queue
    CompletableFuture.runAsync(
        () ->
            executor.execute(
                () -> {
                  try {
                    while (!closed.get()) {
                      var tp = partitionSelector.get();
                      var data = dataSupplier.apply(tp);
                      // throttled data wouldn't put into the queue
                      if (data.stream().allMatch(DataSupplier.Data::throttled)) continue;

                      try {
                        if (queue.offer(data, 5, TimeUnit.SECONDS)
                            && data.stream().allMatch(DataSupplier.Data::done)) return;
                      } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                      }
                    }
                  } finally {
                    closeLatch.countDown();
                    closed.set(true);
                  }
                }));
    return new DataGenerator() {
      @Override
      public void waitForDone() {
        Utils.swallowException(closeLatch::await);
      }

      @Override
      public boolean closed() {
        return closeLatch.getCount() == 0;
      }

      @Override
      public void close() {
        closed.set(true);
        waitForDone();
      }
    };
  }
}
