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
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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

  private final Map<Integer, MBeanClient> mBeanClients = new ConcurrentHashMap<>();
  private final CopyOnWriteArrayList<Map.Entry<Fetcher, BiConsumer<Integer, Exception>>> fetchers =
      new CopyOnWriteArrayList<>();
  private final ScheduledExecutorService executorService;
  private final DelayQueue<DelayedIdentity> delayedWorks;

  // identity (broker id or producer/consumer id) -> beans
  private final ConcurrentMap<Integer, Collection<HasBeanObject>> beans = new ConcurrentHashMap<>();

  public MetricCollectorImpl(
      int threadCount, Duration expiration, Duration interval, Duration cleanerInterval) {
    this.executorService = Executors.newScheduledThreadPool(threadCount + 1);
    this.delayedWorks = new DelayQueue<>();

    // TODO: restart cleaner if it is dead
    // cleaner
    executorService.scheduleWithFixedDelay(
        () -> {
          var before = System.currentTimeMillis() - expiration.toMillis();
          beans
              .values()
              .forEach(
                  bs -> bs.removeIf(hasBeanObject -> hasBeanObject.createdTimestamp() < before));
        },
        cleanerInterval.toMillis(),
        cleanerInterval.toMillis(),
        TimeUnit.MILLISECONDS);

    // processor
    IntStream.range(0, threadCount)
        .forEach(
            i ->
                executorService.submit(
                    () -> {
                      while (!Thread.currentThread().isInterrupted()) {

                        DelayedIdentity identity = null;
                        try {
                          identity = delayedWorks.take();
                          // TODO: employ better sampling mechanism
                          // see
                          // https://github.com/skiptests/astraea/pull/1035#discussion_r1010506993
                          // see
                          // https://github.com/skiptests/astraea/pull/1035#discussion_r1011079711

                          // for each fetcher, perform the fetching and store the metrics
                          for (var fetcher : fetchers) {
                            try {
                              beans
                                  .computeIfAbsent(
                                      identity.id, ignored -> new ConcurrentLinkedQueue<>())
                                  .addAll(fetcher.getKey().fetch(mBeanClients.get(identity.id)));
                            } catch (NoSuchElementException e) {
                              // MBeanClient can throw NoSuchElementException if the result of query
                              // is empty
                              fetcher.getValue().accept(identity.id, e);
                            }
                          }
                        } catch (InterruptedException e) {
                          // swallow the interrupt exception and exit immediately
                          Thread.currentThread().interrupt();
                        } finally {
                          // if we pull out an identity, we must put it back
                          if (identity != null)
                            delayedWorks.put(new DelayedIdentity(interval, identity.id));
                        }
                      }
                    }));
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

  @Override
  public Set<Class<? extends HasBeanObject>> listMetricTypes() {
    return beans.values().stream()
        .flatMap(Collection::stream)
        .map(HasBeanObject::getClass)
        .collect(Collectors.toUnmodifiableSet());
  }

  @Override
  public int size() {
    return beans.values().stream().mapToInt(Collection::size).sum();
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

  public ClusterBean clusterBean() {
    return ClusterBean.of(
        beans.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> List.copyOf(e.getValue()))));
  }

  public Stream<HasBeanObject> metrics() {
    return beans.values().stream().flatMap(Collection::stream);
  }

  @Override
  public void close() {
    // do an interrupt shutdown so sampling threads know it is time to stop
    this.executorService.shutdownNow();
    Utils.packException(() -> this.executorService.awaitTermination(20, TimeUnit.SECONDS));
    this.mBeanClients.forEach((ignore, client) -> client.close());
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
