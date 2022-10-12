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

import java.io.Closeable;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import org.apache.kafka.common.errors.WakeupException;
import org.astraea.common.Utils;
import org.astraea.common.consumer.Consumer;
import org.astraea.common.consumer.ConsumerConfigs;
import org.astraea.common.consumer.ConsumerRebalanceListener;
import org.astraea.common.consumer.SubscribedConsumer;

public class MonkeyThread extends Thread implements Closeable {
  private final Performance.Argument param;
  private final List<ConsumerThread> consumers;
  private final ExecutorService executors;
  private final List<CountDownLatch> closedLatches;
  private boolean closed = false;

  MonkeyThread(
      Performance.Argument param,
      List<ConsumerThread> consumers,
      ExecutorService executors,
      List<CountDownLatch> closedLatches) {
    this.param = param;
    this.consumers = consumers;
    this.executors = executors;
    this.closedLatches = closedLatches;
  }

  public void run() {
    while (!closed) {
      Utils.sleep(param.chaosDuration);
      var type = (int) Math.round(Math.random() * 2);
      var index = (int) (Math.random() * consumers.size());

      if (type == 0 && consumers.size() > 1) { // kill a consumer randomly
        System.out.println("kill a consumer");
        var victimConsumer = consumers.get(index);
        consumers.remove(index);
        closedLatches.remove(index);
        victimConsumer.close();
      } else if (type == 1 && consumers.size() < param.consumers) { // create a new consumer
        System.out.println("add a consumer");
        var consumer = createConsumer();
        consumers.add(consumer);
      } else if (type == 2) { // consumer unsubscribe & resubscribe
        System.out.println("consumer unsubscribe");
        var unsubscribeConsumer = consumers.get(index);
        unsubscribeConsumer.unsubscribe();
        Utils.sleep(param.chaosDuration);
        System.out.println("consumer resubscribe");
        unsubscribeConsumer.resubscribe();
      }
    }
    executors.shutdown();
  }

  private ConsumerThread createConsumer() {
    BiFunction<String, ConsumerRebalanceListener, SubscribedConsumer<byte[], byte[]>>
        consumerSupplier =
            (clientId, listener) ->
                Consumer.forTopics(new HashSet<>(param.topics))
                    .bootstrapServers(param.bootstrapServers())
                    .consumerRebalanceListener(listener)
                    .seek(param.lastOffsets())
                    .configs(param.configs())
                    .config(ConsumerConfigs.GROUP_ID_CONFIG, param.groupId)
                    .config(
                        ConsumerConfigs.ISOLATION_LEVEL_CONFIG,
                        param.transactionSize > 1
                            ? ConsumerConfigs.ISOLATION_LEVEL_COMMITTED
                            : ConsumerConfigs.ISOLATION_LEVEL_UNCOMMITTED)
                    .config(ConsumerConfigs.CLIENT_ID_CONFIG, clientId)
                    .build();

    var clientId = Utils.randomString();
    var consumer =
        consumerSupplier.apply(
            clientId, ps -> ConsumerThread.CLIENT_ID_PARTITIONS.put(clientId, ps));
    var closed = new AtomicBoolean(false);
    var subscribed = new AtomicBoolean(true);
    closedLatches.add(new CountDownLatch(1));
    var closeLatch = closedLatches.get(closedLatches.size() - 1);
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
            ConsumerThread.CLIENT_ID_PARTITIONS.remove(clientId);
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
  }

  @Override
  public void close() {
    closed = true;
  }
}
