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
package org.astraea.common.metrics.collector;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.astraea.common.Utils;
import org.astraea.common.metrics.BeanObject;

public class LocalSenderReceiver implements MetricFetcher.Sender, MetricStore.Receiver {

  public static LocalSenderReceiver of() {
    return new LocalSenderReceiver();
  }

  private final BlockingQueue<Map.Entry<Integer, Collection<BeanObject>>> queue =
      new LinkedBlockingQueue<>();

  private LocalSenderReceiver() {}

  @Override
  public CompletionStage<Void> send(int id, Collection<BeanObject> beans) {
    queue.add(Map.entry(id, beans));
    return CompletableFuture.completedStage(null);
  }

  @Override
  public Map<Integer, Collection<BeanObject>> receive(Duration timeout) {
    var entry = Utils.packException(() -> queue.poll(timeout.toMillis(), TimeUnit.MILLISECONDS));
    if (entry == null) return Map.of();
    return Map.ofEntries(entry);
  }

  public Collection<Map.Entry<Integer, Collection<BeanObject>>> current() {
    return List.copyOf(queue);
  }

  @Override
  public void close() {}
}
