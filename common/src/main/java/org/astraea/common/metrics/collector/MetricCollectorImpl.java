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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.astraea.common.Utils;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.metrics.HasBeanObject;
import org.astraea.common.metrics.MBeanClient;

public class MetricCollectorImpl implements MetricCollector {

  private final Map<Integer, MBeanClient> mBeanClients;
  private final CopyOnWriteArrayList<Map.Entry<Fetcher, BiConsumer<Integer, Exception>>> fetchers;
  private final Duration expiration;
  private final Duration interval;
  private final Map<Class<?>, MetricStorage<?>> storages;
  private final ScheduledExecutorService executorService;
  private final DelayQueue<DelayedIdentity> delayedWorks;
  private final ThreadTimeHighWatermark threadTime;

  public MetricCollectorImpl(
      int threadCount, Duration expiration, Duration interval, Duration cleanerInterval) {
    this.mBeanClients = new ConcurrentHashMap<>();
    this.fetchers = new CopyOnWriteArrayList<>();
    this.expiration = expiration;
    this.interval = interval;
    this.storages = new ConcurrentHashMap<>();
    this.threadTime = new ThreadTimeHighWatermark(threadCount);
    this.executorService = Executors.newScheduledThreadPool(threadCount + 1);
    this.delayedWorks = new DelayQueue<>();

    // TODO: restart cleaner if it is dead
    executorService.scheduleWithFixedDelay(
        clear(), cleanerInterval.toMillis(), cleanerInterval.toMillis(), TimeUnit.MILLISECONDS);
    IntStream.range(0, threadCount).forEach(i -> executorService.submit(process(i)));
  }

  @Override
  public void addFetcher(Fetcher fetcher, BiConsumer<Integer, Exception> noSuchMetricHandler) {
    this.fetchers.add(Map.entry(fetcher, noSuchMetricHandler));
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
  public Collection<Fetcher> listFetchers() {
    return fetchers.stream().map(Map.Entry::getKey).collect(Collectors.toList());
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
          if (client != null) throw new IllegalArgumentException(errorMessage.get());
          else return clientSupplier.get();
        });
    this.delayedWorks.put(new DelayedIdentity(Duration.ZERO, identity));
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T extends HasBeanObject> List<T> metrics(Class<T> metricClass, int identity, long since) {
    return (List<T>)
        (storages.computeIfAbsent(metricClass, (ignore) -> new MetricStorage<>(metricClass)))
                .storage
                .getOrDefault(identity, MetricStorage.emptyStorage())
                // query range [since, threadTime)
                // It's design as a half-open interval for a very specific reason.
                // Other threads might insert metrics at the minimum thread time moment.
                // Have to exclude that point of metrics, to prevent client miss any metrics.
                .subMap(since, true, threadTime.read(), false)
                .values()
                .stream()
                .flatMap(Collection::stream)
                .collect(Collectors.toUnmodifiableList());
  }

  @Override
  public long storageSize(int identity) {
    return storages.values().stream()
        .map(x -> x.storage.getOrDefault(identity, null))
        .filter(Objects::nonNull)
        .mapToLong(ConcurrentNavigableMap::size)
        .sum();
  }

  @Override
  public ClusterBean clusterBean() {
    Map<Integer, Collection<HasBeanObject>> metrics =
        storages.values().stream()
            .map(x -> x.storage)
            .map(Map::entrySet)
            .flatMap(Collection::stream)
            .collect(
                Collectors.groupingBy(
                    Map.Entry::getKey,
                    Collectors.mapping(
                        Map.Entry::getValue,
                        Collectors.mapping(
                            ConcurrentNavigableMap::values,
                            Collectors.flatMapping(
                                x -> (Stream<HasBeanObject>) x.stream().flatMap(y -> y.stream()),
                                Collectors.toCollection(ArrayList::new))))));
    return ClusterBean.of(metrics);
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
  private Runnable process(int threadId) {
    return () -> {
      while (!Thread.currentThread().isInterrupted()) {
        DelayedIdentity identity = null;
        try {
          identity = delayedWorks.poll(5, TimeUnit.MILLISECONDS);
          if (identity == null) {
            threadTime.update(threadId, System.currentTimeMillis());
            continue;
          }
          var id = identity.id();
          var client = mBeanClients.get(id);

          // TODO: employ better sampling mechanism
          // see https://github.com/skiptests/astraea/pull/1035#discussion_r1010506993
          // see https://github.com/skiptests/astraea/pull/1035#discussion_r1011079711

          // for each fetcher, perform the fetching and store the metrics
          fetchers.stream()
              .map(
                  entry -> {
                    try {
                      return entry.getKey().fetch(client);
                    } catch (NoSuchElementException e) {
                      entry.getValue().accept(id, e);
                      return Collections.<HasBeanObject>emptyList();
                    }
                  })
              // Intentional sleep, do not remove
              .peek(i -> Utils.packException(() -> TimeUnit.MILLISECONDS.sleep(1)))
              .peek(i -> threadTime.update(threadId, System.currentTimeMillis()))
              .forEach(metrics -> store(id, metrics));
        } catch (RuntimeException e) {
          if (e.getCause() instanceof InterruptedException)
            // swallow the interrupt exception and exit immediately
            Thread.currentThread().interrupt();
          else e.printStackTrace();
        } catch (InterruptedException e) {
          // swallow the interrupt exception and exit immediately
          Thread.currentThread().interrupt();
        } catch (Exception e) {
          e.printStackTrace();
        } finally {
          // if we pull out an identity, we must put it back
          if (identity != null) delayedWorks.put(new DelayedIdentity(interval, identity.id()));
        }
      }
    };
  }

  /** Return a {@link Runnable} that clears old metrics */
  private Runnable clear() {
    return () -> {
      try {
        var before = System.currentTimeMillis() - expiration.toMillis();
        for (var storage : this.storages.values()) storage.clear(before);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (Exception e) {
        e.printStackTrace();
      }
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
    private static final ConcurrentNavigableMap<Long, ConcurrentLinkedQueue<?>> EMPTY_STORAGE =
        new ConcurrentSkipListMap<>();

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static <T> ConcurrentNavigableMap<Long, ConcurrentLinkedQueue<T>> emptyStorage() {
      return ((ConcurrentSkipListMap) EMPTY_STORAGE);
    }

    private final Class<T> theClass;
    private final Map<Integer, ConcurrentNavigableMap<Long, ConcurrentLinkedQueue<T>>> storage;
    private final ReentrantLock cleanerLock;

    public MetricStorage(Class<T> theClass) {
      this.theClass = theClass;
      this.cleanerLock = new ReentrantLock();
      this.storage = new ConcurrentHashMap<>();
    }

    @SuppressWarnings("unchecked")
    public void put(int identity, HasBeanObject metric) {
      storage
          .computeIfAbsent(identity, (ignore) -> new ConcurrentSkipListMap<>())
          .computeIfAbsent(metric.createdTimestamp(), (ignore) -> new ConcurrentLinkedQueue<>())
          .add((T) metric);
    }

    /** Scanning from the last metrics, delete any metrics that is sampled before the given time. */
    public void clear(long before) throws InterruptedException {
      try {
        cleanerLock.lockInterruptibly();
        storage.forEach(
            (identity, map) ->
                map.entrySet().stream()
                    .takeWhile(x -> x.getKey() < before)
                    .forEach(x -> map.remove(x.getKey())));
      } finally {
        if (cleanerLock.isHeldByCurrentThread()) cleanerLock.unlock();
      }
    }

    public Class<T> metricClass() {
      return theClass;
    }
  }

  public static class Builder {

    private int threadCount = Runtime.getRuntime().availableProcessors();
    private Duration expiration = Duration.ofMinutes(3);
    private Duration interval = Duration.ofSeconds(1);
    private Duration cleanerInterval = Duration.ofSeconds(30);

    Builder() {}

    public Builder threads(int threads) {
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

    private final long deadlineNs;

    private DelayedIdentity(Duration delay, int id) {
      this.deadlineNs = delay.toNanos() + System.nanoTime();
      this.id = id;
    }

    private final int id;

    public int id() {
      return id;
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

  /** Tracking the metric timestamp that is safe to expose. */
  private static class ThreadTimeHighWatermark {

    private final AtomicLongArray threadTimes;
    private final AtomicLong highWatermark;

    public ThreadTimeHighWatermark(int threadCount) {
      final var defaultTime = new long[threadCount];
      Arrays.fill(defaultTime, Long.MAX_VALUE);
      this.threadTimes = new AtomicLongArray(defaultTime);
      this.highWatermark = new AtomicLong(Long.MAX_VALUE);
    }

    /** set thread time for specific thread id, and update the high watermark. */
    public void update(int threadId, long threadTime) {
      threadTimes.set(threadId, threadTime);
      long min = threadTimes.get(0);
      for (int i = 1; i < threadTimes.length(); i++) min = Math.min(min, threadTimes.get(i));
      highWatermark.set(min);
    }

    /**
     * @return the high watermark
     */
    public long read() {
      return highWatermark.get();
    }
  }
}
