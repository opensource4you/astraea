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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.common.Utils;
import org.astraea.common.metrics.HasBeanObject;
import org.astraea.common.metrics.MBeanClient;

public class MetricCollectorImpl implements MetricCollector {

  private final Map<Integer, MBeanClient> mBeanClients;
  private final CopyOnWriteArrayList<Map.Entry<Fetcher, Set<Integer>>> fetchers;
  private final Duration expiration;
  private final Duration interval;
  private final Map<Class<?>, MetricStorage<?>> storages;
  private final ScheduledExecutorService executorService;
  private final DelayQueue<DelayedIdentity> delayedWorks;

  public MetricCollectorImpl(
      int threadCount, Duration expiration, Duration interval, Duration cleanerInterval) {
    this.mBeanClients = new ConcurrentHashMap<>();
    this.fetchers = new CopyOnWriteArrayList<>();
    this.expiration = expiration;
    this.interval = interval;
    this.storages = new ConcurrentHashMap<>();
    this.executorService = Executors.newScheduledThreadPool(threadCount);
    this.delayedWorks = new DelayQueue<>();

    // TODO: restart cleaner if it is dead
    executorService.scheduleWithFixedDelay(
        clear(), cleanerInterval.toMillis(), cleanerInterval.toMillis(), TimeUnit.MILLISECONDS);
    IntStream.range(0, threadCount).forEach(i -> executorService.submit(process()));
  }

  @Override
  public void addFetcher(Set<Integer> identities, Fetcher fetcher) {
    this.fetchers.add(Map.entry(fetcher, identities));
  }

  @Override
  public void registerJmx(int identity, InetSocketAddress socketAddress) {
    this.registerJmx(
        identity,
        () -> MBeanClient.jndi(socketAddress.getHostName(), socketAddress.getPort()),
        () ->
            "Attempt to register identity "
                + identity
                + " with address "
                + socketAddress
                + ". But this id is already registered");
  }

  @Override
  public void registerLocalJmx(int identity) {
    this.registerJmx(
        identity,
        MBeanClient::local,
        () ->
            "Attempt to register identity "
                + identity
                + " with the local JMX server. But this id is already registered");
  }

  @Override
  public Map<Fetcher, Set<Integer>> listFetchers() {
    return fetchers.stream()
        .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  @Override
  public Set<Integer> listIdentities() {
    return mBeanClients.keySet();
  }

  @SuppressWarnings("resource")
  private void registerJmx(
      int identity, Supplier<MBeanClient> clientSupplier, Supplier<String> errorMessage) {
    mBeanClients.compute(
        identity,
        (id, client) -> {
          if (client != null) throw new IllegalStateException(errorMessage.get());
          else return clientSupplier.get();
        });
    this.delayedWorks.put(new DelayedIdentity(interval, identity));
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T extends HasBeanObject> Map<Integer, Collection<T>> metrics(Class<T> metricClass) {
    return ((MetricStorage<T>)
            storages.computeIfAbsent(metricClass, (ignore) -> new MetricStorage<>(metricClass)))
        .view();
  }

  /** Store the metrics into the storage of specific identity */
  private void store(int identity, Collection<? extends HasBeanObject> metrics) {
    metrics.forEach(
        (metric) ->
            storages
                .computeIfAbsent(
                    metric.getClass(), (ignore) -> new MetricStorage<>(metric.getClass()))
                .put(identity, metric));
  }

  /** Return a {@link Runnable} that perform the metric sampling task */
  private Runnable process() {
    return () -> {
      while (!Thread.currentThread().isInterrupted()) {
        DelayedIdentity identity = null;
        try {
          // take an identity, this is a blocking method
          identity = delayedWorks.take();
          var id = identity.id();
          var client = mBeanClients.get(id);

          // for each fetcher, perform the fetching and store the metrics
          fetchers.stream()
              .filter(entry -> entry.getValue().contains(id))
              .map(Map.Entry::getKey)
              .map(f -> f.fetch(client))
              .forEach(metrics -> store(id, metrics));
        } catch (InterruptedException e) {
          // swallow the interrupt exception and exit immediately
          Thread.currentThread().interrupt();
        } catch (Exception e) {
          e.printStackTrace();
        } finally {
          // if we pull out an identity, we must put it back
          if (identity != null) delayedWorks.put(identity.next());
        }
      }
    };
  }

  /** Return a {@link Runnable} that clears old metrics */
  private Runnable clear() {
    return () -> {
      // TODO: test if this thread stop on time when we shutdown called
      var before = System.currentTimeMillis() - expiration.toMillis();
      this.storages.values().forEach(storage -> storage.clear(before));
    };
  }

  @Override
  public void close() {
    // do an interrupt shutdown so sampling threads know it is time to stop
    this.executorService.shutdownNow();
    Utils.packException(() -> this.executorService.awaitTermination(20, TimeUnit.SECONDS));
    this.mBeanClients.forEach((ignore, client) -> client.close());
  }

  private static class MetricStorage<T extends HasBeanObject> {
    private final Class<T> theClass;
    private final Map<Integer, ConcurrentSkipListMap<Long, T>> storage;
    private final Map<Integer, AtomicLong> top;
    private final ReentrantLock cleanerLock;

    public MetricStorage(Class<T> theClass) {
      this.theClass = theClass;
      this.top = new ConcurrentHashMap<>();
      this.cleanerLock = new ReentrantLock();
      this.storage = new ConcurrentHashMap<>();
    }

    private long nextIndex(int identity) {
      return top.computeIfAbsent(identity, (ignore) -> new AtomicLong(0)).getAndIncrement();
    }

    @SuppressWarnings("unchecked")
    public void put(int identity, Object metric) {
      storage
          .computeIfAbsent(identity, (ignore) -> new ConcurrentSkipListMap<>())
          .put(nextIndex(identity), (T) metric);
    }

    /** Scanning from the last metrics, delete any metrics that is sampled before the given time. */
    public void clear(long before) {
      try {
        Utils.packException(cleanerLock::lockInterruptibly);
        storage.forEach(
            (identity, map) ->
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

    private int threadCount = Runtime.getRuntime().availableProcessors();
    private Duration expiration = Duration.ofMinutes(3);
    private Duration interval = Duration.ofSeconds(1);
    private Duration cleanerInterval = Duration.ofSeconds(30);

    Builder() {}

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

    public Builder cleanerInterval(Duration interval) {
      this.cleanerInterval = Objects.requireNonNull(interval);
      return this;
    }

    public MetricCollector build() {
      return new MetricCollectorImpl(threadCount, expiration, interval, cleanerInterval);
    }
  }

  private static class DelayedIdentity implements Delayed {

    private final Duration delay;
    private final long deadlineNs;

    private DelayedIdentity(Duration delay, int id) {
      this.deadlineNs = delay.toNanos() + System.nanoTime();
      this.delay = delay;
      this.id = id;
    }

    private final int id;

    public int id() {
      return id;
    }

    public DelayedIdentity next() {
      return new DelayedIdentity(delay, id);
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
