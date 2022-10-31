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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.astraea.common.Utils;
import org.astraea.common.consumer.Consumer;
import org.astraea.common.consumer.ConsumerConfigs;

public class MonkeyThread implements AbstractThread {

  private final CountDownLatch closeLatch;
  private final AtomicBoolean close;

  MonkeyThread(AtomicBoolean close) {
    this.close = close;
    this.closeLatch = new CountDownLatch(1);
  }

  static List<MonkeyThread> play(List<ConsumerThread> consumerThreads, Performance.Argument param) {
    if (param.monkeys == null) return List.of();
    System.out.println("create chaos monkey");
    return param.monkeys.entrySet().stream()
        .map(
            entry -> {
              switch ((entry.getKey())) {
                case "kill":
                  return killMonkey(consumerThreads, entry.getValue());
                case "add":
                  return addMonkey(consumerThreads, entry.getValue(), param);
                default:
                  return unsubscribeMonkey(consumerThreads, entry.getValue());
              }
            })
        .collect(Collectors.toUnmodifiableList());
  }

  private static MonkeyThread killMonkey(List<ConsumerThread> consumerThreads, Duration frequency) {
    var close = new AtomicBoolean(false);
    CompletableFuture.runAsync(
        () -> {
          while (!close.get()) {
            if (consumerThreads.size() > 1) {
              System.out.println("kill a consumer");
              var consumer = consumerThreads.remove((int) (Math.random() * consumerThreads.size()));
              consumer.close();
              consumer.waitForDone();
              Utils.sleep(frequency);
            }
          }
        });
    return new MonkeyThread(close);
  }

  private static MonkeyThread addMonkey(
      List<ConsumerThread> consumerThreads, Duration frequency, Performance.Argument param) {
    var close = new AtomicBoolean(false);
    CompletableFuture.runAsync(
        () -> {
          while (!close.get()) {
            if (consumerThreads.size() < param.consumers) {
              System.out.println("add a consumer");
              var consumer =
                  ConsumerThread.create(
                          1,
                          (clientId, listener) ->
                              (param.pattern == null
                                      ? Consumer.forTopics(Set.copyOf(param.topics))
                                      : Consumer.forTopics(param.pattern))
                                  .configs(param.configs())
                                  .config(
                                      ConsumerConfigs.ISOLATION_LEVEL_CONFIG,
                                      param.transactionSize > 1
                                          ? ConsumerConfigs.ISOLATION_LEVEL_COMMITTED
                                          : ConsumerConfigs.ISOLATION_LEVEL_UNCOMMITTED)
                                  .bootstrapServers(param.bootstrapServers())
                                  .config(ConsumerConfigs.GROUP_ID_CONFIG, param.groupId)
                                  .seek(param.lastOffsets())
                                  .consumerRebalanceListener(listener)
                                  .config(ConsumerConfigs.CLIENT_ID_CONFIG, clientId)
                                  .build())
                      .get(0);
              consumerThreads.add(consumer);
              Utils.sleep(frequency);
            }
          }
        });
    return new MonkeyThread(close);
  }

  private static MonkeyThread unsubscribeMonkey(
      List<ConsumerThread> consumerThreads, Duration frequency) {
    var close = new AtomicBoolean(false);
    CompletableFuture.runAsync(
        () -> {
          while (!close.get()) {
            var thread = consumerThreads.get((int) (Math.random() * consumerThreads.size()));

            System.out.println("unsubscribe consumer");
            thread.unsubscribe();
            Utils.sleep(frequency);
            System.out.println("resubscribe consumer");
            thread.resubscribe();
          }
        });
    return new MonkeyThread(close);
  }

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
    close.set(true);
    closeLatch.countDown();
  }
}
