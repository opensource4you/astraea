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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import org.astraea.common.Utils;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.metrics.BeanQuery;
import org.astraea.common.metrics.MBeanClient;

public interface MetricsFetcher extends AutoCloseable {

  static Builder builder() {
    return new Builder();
  }

  /**
   * @return the latest beans
   */
  Map<Integer, Collection<BeanObject>> latest();

  /**
   * @return the latest fetched identities
   */
  Set<Integer> identities();

  @Override
  void close();

  interface Sender extends AutoCloseable {

    static Sender local() {
      return LocalSenderReceiver.of();
    }

    CompletionStage<Void> send(int id, Collection<BeanObject> beans);

    @Override
    default void close() {}
  }

  class Builder {

    private int threads = 4;

    private Duration fetchBeanDelay = Duration.ofSeconds(1);
    private Duration fetchMetadataDelay = Duration.ofMinutes(5);
    private Sender sender;
    private Supplier<CompletionStage<Map<Integer, MBeanClient>>> clientSupplier;

    private Builder() {}

    public Builder threads(int threads) {
      this.threads = threads;
      return this;
    }

    public Builder fetchBeanDelay(Duration fetchBeanDelay) {
      this.fetchBeanDelay = fetchBeanDelay;
      return this;
    }

    public Builder fetchMetadataDelay(Duration fetchMetadataDelay) {
      this.fetchMetadataDelay = fetchMetadataDelay;
      return this;
    }

    public Builder sender(Sender sender) {
      this.sender = sender;
      return this;
    }

    public Builder clientSupplier(
        Supplier<CompletionStage<Map<Integer, MBeanClient>>> clientSupplier) {
      this.clientSupplier = clientSupplier;
      return this;
    }

    public MetricsFetcher build() {
      return new MetricsFetcherImpl(
          threads,
          Objects.requireNonNull(fetchBeanDelay, "fetchBeanDelay can't be null"),
          Objects.requireNonNull(fetchMetadataDelay, "fetchMetadataDelay can't be null"),
          Objects.requireNonNull(sender, "sends can't be null"),
          Objects.requireNonNull(clientSupplier, "clientSupplier can't be null"));
    }
  }

  class MetricsFetcherImpl implements MetricsFetcher {
    private volatile Map<Integer, MBeanClient> clients = new HashMap<>();

    private final Map<Integer, Collection<BeanObject>> latest = new ConcurrentHashMap<>();

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final DelayQueue<DelayedIdentity> works = new DelayQueue<>();

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final Sender sender;

    private final ExecutorService executor;

    private final Supplier<CompletionStage<Map<Integer, MBeanClient>>> clientSupplier;

    private final Duration fetchBeanDelay;

    private MetricsFetcherImpl(
        int threads,
        Duration fetchBeanDelay,
        Duration fetchMetadataDelay,
        Sender sender,
        Supplier<CompletionStage<Map<Integer, MBeanClient>>> clientSupplier) {
      this.fetchBeanDelay = fetchBeanDelay;
      this.sender = sender;
      this.clientSupplier = clientSupplier;
      this.executor = Executors.newFixedThreadPool(threads);
      var metadataUpdateId = -((int) System.currentTimeMillis());
      works.put(new DelayedIdentity(Duration.ZERO, metadataUpdateId));
      Runnable job =
          () -> {
            try {
              while (!closed.get()) {
                var identity = works.take();
                try {
                  if (identity.id == metadataUpdateId) {
                    updateMetadata();
                    continue;
                  }
                  updateData(identity);
                } catch (Exception e) {
                  // TODO: it needs better error handling
                  e.printStackTrace();
                } finally {
                  works.put(
                      new DelayedIdentity(
                          identity.id == metadataUpdateId ? fetchMetadataDelay : fetchBeanDelay,
                          identity.id));
                }
              }
            } catch (InterruptedException ex) {
              // swallow
            }
          };

      IntStream.range(0, threads).forEach(ignored -> executor.execute(job));
    }

    private void updateMetadata() {
      clientSupplier
          .get()
          .whenCompleteAsync(
              (r, e) -> {
                if (e != null) {
                  // TODO: it needs better error handling
                  e.printStackTrace();
                  return;
                }
                lock.writeLock().lock();
                Map<Integer, MBeanClient> old;
                try {
                  old = clients;
                  clients = r;
                  works.clear();
                  clients.forEach(
                      (id, client) -> works.put(new DelayedIdentity(fetchBeanDelay, id)));
                } finally {
                  lock.writeLock().unlock();
                }
                old.values().forEach(c -> Utils.swallowException(c::close));
              });
    }

    private void updateData(DelayedIdentity identity) {
      lock.readLock().lock();
      Collection<BeanObject> beans;
      try {
        beans = clients.get(identity.id).beans(BeanQuery.all(), e -> {});
      } finally {
        lock.readLock().unlock();
      }
      latest.put(identity.id, beans);
      sender.send(identity.id, beans);
    }

    @Override
    public Map<Integer, Collection<BeanObject>> latest() {
      return Map.copyOf(latest);
    }

    @Override
    public Set<Integer> identities() {
      return Set.copyOf(clients.keySet());
    }

    @Override
    public void close() {
      closed.set(true);
      executor.shutdownNow();
      Utils.packException(() -> executor.awaitTermination(30, TimeUnit.SECONDS));
      clients.values().forEach(c -> Utils.swallowException(c::close));
      sender.close();
    }
  }

  class DelayedIdentity implements Delayed {
    private final long deadlineNs;
    private final int id;

    private DelayedIdentity(Duration delay, int id) {
      this.deadlineNs = delay.toNanos() + System.nanoTime();
      this.id = id;
    }

    @Override
    public long getDelay(TimeUnit timeUnit) {
      return timeUnit.convert(deadlineNs - System.nanoTime(), TimeUnit.NANOSECONDS);
    }

    @Override
    public int compareTo(Delayed delayed) {
      return Long.compare(
          this.getDelay(TimeUnit.NANOSECONDS), delayed.getDelay(TimeUnit.NANOSECONDS));
    }
  }
}
