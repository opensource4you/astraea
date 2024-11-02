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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.LongStream;
import org.astraea.common.Configuration;
import org.astraea.common.DataUnit;
import org.astraea.common.Utils;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.producer.Record;
import org.astraea.common.producer.RecordGenerator;

interface DataGenerator extends AbstractThread {
  static DataGenerator of(
      BlockingQueue<List<Record<byte[], byte[]>>> queue,
      Supplier<TopicPartition> partitionSelector,
      Performance.Argument argument) {
    var keyDistConfig = new Configuration(argument.keyDistributionConfig);
    var keySizeDistConfig = new Configuration(argument.keySizeDistributionConfig);
    var valueDistConfig = new Configuration(argument.valueDistributionConfig);
    var dataSupplier =
        RecordGenerator.builder()
            .batchSize(argument.transactionSize)
            .keyTableSeed(argument.recordKeyTableSeed)
            .keyRange(LongStream.rangeClosed(0, 10000).boxed().toList())
            .keyDistribution(argument.keyDistributionType.create(10000, keyDistConfig))
            .keySizeDistribution(
                argument.keySizeDistributionType.create(
                    (int) argument.keySize.bytes(), keySizeDistConfig))
            .valueTableSeed(argument.recordValueTableSeed)
            .valueRange(LongStream.rangeClosed(0, 10000).boxed().toList())
            .valueDistribution(argument.valueDistributionType.create(10000, valueDistConfig))
            .valueSizeDistribution(
                argument.valueDistributionType.create(
                    argument.valueSize.measurement(DataUnit.Byte).intValue(), valueDistConfig))
            .throughput(tp -> argument.throttles.getOrDefault(tp, argument.throughput))
            .build();
    var closed = new AtomicBoolean(false);
    var start = System.currentTimeMillis();

    // put the data into blocking queue
    var future =
        CompletableFuture.runAsync(
            () -> {
              try {
                long dataCount = 0;
                while (!closed.get()) {
                  // check the generator is finished or not
                  if (argument.exeTime.percentage(dataCount, System.currentTimeMillis() - start)
                      >= 100D) return;
                  var tp = partitionSelector.get();
                  var records = dataSupplier.apply(tp);
                  dataCount += records.size();

                  // throttled data wouldn't put into the queue
                  if (records.isEmpty()) continue;
                  queue.put(records);
                }
              } catch (InterruptedException e) {
                throw new RuntimeException("The data generator didn't close properly", e);
              } finally {
                closed.set(true);
              }
            });

    return new DataGenerator() {
      @Override
      public void waitForDone() {
        Utils.swallowException(future::join);
      }

      @Override
      public boolean closed() {
        return future.isDone();
      }

      @Override
      public void close() {
        closed.set(true);
        waitForDone();
      }
    };
  }
}
