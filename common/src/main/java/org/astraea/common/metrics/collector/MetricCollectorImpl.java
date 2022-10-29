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

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import org.astraea.common.Utils;
import org.astraea.common.metrics.HasBeanObject;
import org.astraea.common.metrics.MBeanClient;

public class MetricCollectorImpl implements MetricCollector {

  private final Map<Integer, InetSocketAddress> jmx;
  private final Map<Integer, ManagedMBeanClient> mBeanClients;
  private final Duration cleanerInterval = Duration.ofSeconds(30);
  private final Duration expiration;
  private final Duration interval;
  private final Map<Class<?>, MetricStorage<?>> storages;
  private final ScheduledExecutorService executorService;
  private final Set<ScheduledFuture<?>> scheduledTasks;

  public MetricCollectorImpl(
      Map<Integer, InetSocketAddress> jmx,
      ScheduledExecutorService executorService,
      Duration expiration,
      Duration interval) {
    this.jmx = Map.copyOf(jmx);
    this.expiration = expiration;
    this.interval = interval;
    this.storages = new ConcurrentHashMap<>();
    this.executorService = executorService;
    this.mBeanClients =
        jmx.entrySet().stream()
            .collect(
                Collectors.toConcurrentMap(
                    Map.Entry::getKey, entry -> new ManagedMBeanClient(entry.getValue())));
    this.scheduledTasks = new ConcurrentSkipListSet<>();

    // TODO: if the task failed, will it stop or keep on scheduling?
    this.scheduledTasks.add(
        executorService.scheduleWithFixedDelay(
            this::cleaning,
            cleanerInterval.toMillis(),
            cleanerInterval.toMillis(),
            TimeUnit.MILLISECONDS));
  }

  @Override
  public void register(Fetcher fetcher) {
    var firstDelay = ThreadLocalRandom.current().nextLong(0, interval.toMillis());

    this.mBeanClients.forEach(
        (id, client) ->
            this.scheduledTasks.add(
                executorService.scheduleWithFixedDelay(
                    () -> {
                      Collection<? extends HasBeanObject> metrics;
                      try (ManagedMBeanClient.Ownership ownership = client.claim()) {
                        metrics = fetcher.fetch(ownership.client());
                      }
                      store(id, metrics);
                    },
                    firstDelay,
                    interval.toMillis(),
                    TimeUnit.MILLISECONDS)));
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T extends HasBeanObject> Map<Integer, Collection<T>> metrics(Class<T> metricClass) {
    return ((MetricStorage<T>)
            storages.computeIfAbsent(metricClass, (ignore) -> new MetricStorage<>(metricClass)))
        .view();
  }

  /** Store the metrics into the storage of specific broker */
  private void store(int broker, Collection<? extends HasBeanObject> metrics) {
    metrics.forEach(
        (metric) ->
            storages
                .computeIfAbsent(
                    metric.getClass(), (ignore) -> new MetricStorage<>(metric.getClass()))
                .put(broker, metric));
  }

  private void cleaning() {
    var before = System.currentTimeMillis() - expiration.toMillis();
    this.storages.values().forEach(storage -> storage.clear(before));
  }

  @Override
  public void close() {
    this.scheduledTasks.forEach(x -> x.cancel(false));
    this.executorService.shutdown();
    Utils.packException(() -> this.executorService.awaitTermination(20, TimeUnit.SECONDS));
    this.mBeanClients.forEach((ignore, client) -> client.close());
  }

  private static class ManagedMBeanClient implements AutoCloseable {
    private final MBeanClient client;
    private final ReentrantLock lock;

    ManagedMBeanClient(InetSocketAddress socketAddress) {
      this.client = MBeanClient.jndi(socketAddress.getHostName(), socketAddress.getPort());
      this.lock = new ReentrantLock();
    }

    /**
     * retrieve the right to use the client.
     *
     * @return a {@link AutoCloseable} that can release the lock.
     */
    public Ownership claim() {
      Utils.packException(lock::lockInterruptibly);
      return new Ownership();
    }

    @Override
    public void close() {
      try (Ownership ownership = claim()) {
        ownership.client().close();
      }
    }

    public class Ownership implements AutoCloseable {

      public MBeanClient client() {
        return client;
      }

      @Override
      public void close() {
        lock.unlock();
      }
    }
  }

  private class MetricStorage<T extends HasBeanObject> {
    private final Class<T> theClass;
    private final Map<Integer, ConcurrentSkipListMap<Long, T>> storage;
    private final Map<Integer, AtomicLong> top;
    private final ReentrantLock cleanerLock;

    public MetricStorage(Class<T> theClass) {
      this.theClass = theClass;
      this.top = new ConcurrentHashMap<>();
      this.cleanerLock = new ReentrantLock();
      this.storage = new ConcurrentHashMap<>();
      MetricCollectorImpl.this
          .jmx
          .keySet()
          .forEach(id -> storage.putIfAbsent(id, new ConcurrentSkipListMap<>()));
    }

    private long nextIndex(int broker) {
      return top.computeIfAbsent(broker, (ignore) -> new AtomicLong(0)).getAndIncrement();
    }

    @SuppressWarnings("unchecked")
    public void put(int broker, Object metric) {
      storage
          .computeIfAbsent(broker, (ignore) -> new ConcurrentSkipListMap<>())
          .put(nextIndex(broker), (T) metric);
    }

    /** Scanning from the last metrics, delete any metrics that is sampled before the given time. */
    public void clear(long before) {
      try {
        Utils.packException(cleanerLock::lockInterruptibly);
        storage.forEach(
            (broker, map) ->
                map.entrySet().stream()
                    .takeWhile(entry -> entry.getValue().createdTimestamp() < before)
                    .forEach(entry -> map.remove(entry.getKey())));
      } finally {
        cleanerLock.unlock();
      }
    }

    public Class<T> metricClass() {
      return theClass;
    }

    public Map<Integer, Collection<T>> view() {
      return storage.entrySet().stream()
          .collect(
              Collectors.toUnmodifiableMap(
                  Map.Entry::getKey,
                  entry -> Collections.unmodifiableCollection(entry.getValue().values())));
    }
  }

  public static class Builder {

    private Map<Integer, InetSocketAddress> brokerJmxAddresses;
    private int threadCount = Runtime.getRuntime().availableProcessors();
    private ScheduledExecutorService executorService = null;
    private Duration expiration = Duration.ofMinutes(3);
    private Duration interval = Duration.ofSeconds(1);

    Builder() {}

    public Builder jmx(Map<Integer, InetSocketAddress> address) {
      this.brokerJmxAddresses = Objects.requireNonNull(address);
      return this;
    }

    public Builder executor(ScheduledExecutorService executorService) {
      this.executorService = executorService;
      return this;
    }

    public Builder executor(int threads) {
      this.threadCount = Utils.requirePositive(threads);
      return this;
    }

    public Builder expiration(Duration expiration) {
      this.expiration = Objects.requireNonNull(expiration);
      return this;
    }

    public Builder interval(Duration interval) {
      this.interval = Objects.requireNonNull(interval);
      return this;
    }

    public MetricCollector build() {
      if (executorService == null)
        this.executorService = Executors.newScheduledThreadPool(threadCount);

      return new MetricCollectorImpl(brokerJmxAddresses, executorService, expiration, interval);
    }
  }
}
