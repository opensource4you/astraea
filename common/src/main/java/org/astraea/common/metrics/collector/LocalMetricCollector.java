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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
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

/**
 * Keep fetching Jmx mbeans from given targets. Then store the fetched metrics temporary and
 * locally.
 *
 * <p>The following code fetch mbeans from local jmx server
 *
 * <pre>{@code
 * try (var collector =
 *     MetricCollector.local()
 *         .registerLocalJmx(0)
 *         .addMetricSensor(client -> List.of(HostMetrics.jvmMemory(client)))
 *         .interval(sample)
 *         .build()) {
 *   collector.clusterBean();
 * }
 * }</pre>
 */
public class LocalMetricCollector implements MetricCollector {
  private final Map<Integer, MBeanClient> mBeanClients;
  private final List<Map.Entry<MetricSensor, BiConsumer<Integer, Exception>>> sensors;
  private final ScheduledExecutorService executorService;
  private final DelayQueue<DelayedIdentity> delayedWorks;

  // identity (broker id or producer/consumer id) -> beans
  private final ConcurrentMap<Integer, Collection<HasBeanObject>> beans = new ConcurrentHashMap<>();

  public LocalMetricCollector(
      int threadCount,
      Duration expiration,
      Duration interval,
      Duration cleanerInterval,
      Map<Integer, MBeanClient> mBeanClients,
      List<Map.Entry<MetricSensor, BiConsumer<Integer, Exception>>> sensors) {
    this.executorService = Executors.newScheduledThreadPool(threadCount + 1);
    this.delayedWorks = new DelayQueue<>();
    this.mBeanClients = mBeanClients;
    this.sensors = sensors;
    mBeanClients.forEach(
        (id, ignore) -> this.delayedWorks.put(new DelayedIdentity(Duration.ZERO, id)));

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

                          // for each sensor, perform the fetching and store the metrics
                          var clusterBean = clusterBean();
                          for (var sensor : sensors) {
                            try {
                              var newBeans =
                                  sensor.getKey().fetch(mBeanClients.get(identity.id), clusterBean);
                              beans
                                  .computeIfAbsent(
                                      identity.id, ignored -> new ConcurrentLinkedQueue<>())
                                  .addAll(newBeans);
                            } catch (NoSuchElementException e) {
                              // MBeanClient can throw NoSuchElementException if the result of query
                              // is empty
                              sensor.getValue().accept(identity.id, e);
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
  public Collection<MetricSensor> metricSensors() {
    return sensors.stream().map(Map.Entry::getKey).collect(Collectors.toList());
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

  public ClusterBean clusterBean() {
    return ClusterBean.of(
        beans.entrySet().stream()
            .collect(
                Collectors.toUnmodifiableMap(Map.Entry::getKey, e -> List.copyOf(e.getValue()))));
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

    private final HashMap<Integer, Supplier<MBeanClient>> mBeanClients = new HashMap<>();

    private final List<Map.Entry<MetricSensor, BiConsumer<Integer, Exception>>> sensors =
        new ArrayList<>();

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

    /**
     * Create a Jmx connection with the given `socketAddress` and tag it as a user-defined identity.
     * Register with the same identity will cause `IllegalArgumentException`.
     *
     * @param identity identity of the registered connection
     * @param socketAddress address of the jmx server to connect
     * @throws IllegalArgumentException when the given identity has been registered before
     * @return this builder
     */
    public Builder registerJmx(Integer identity, InetSocketAddress socketAddress) {
      mBeanClients.compute(
          identity,
          (id, client) -> {
            if (client != null)
              throw new IllegalArgumentException(
                  "Attempt to register identity "
                      + identity
                      + " with address "
                      + socketAddress
                      + ". But this id is already registered");
            else
              return () -> MBeanClient.jndi(socketAddress.getHostName(), socketAddress.getPort());
          });
      return this;
    }

    /**
     * Create Jmx connections with all the given address. It is equivalent to subsequent call to
     * {@link Builder#registerJmx(Integer, InetSocketAddress)}.
     *
     * @param idAddress identities and corresponding address to register
     * @return this builder
     */
    public Builder registerJmxs(Map<Integer, InetSocketAddress> idAddress) {
      idAddress.forEach(this::registerJmx);
      return this;
    }

    /**
     * Register a {@link MetricSensor}.
     *
     * <p>Note that sensors will be used by every identity. It is possible that the metric this
     * {@link MetricSensor} is sampling doesn't exist on a JMX server(For example: sampling Kafka
     * broker metric from a Producer client). When such case occurred. The {@code
     * noSuchMetricHandler} will be invoked.
     *
     * @param metricSensor the sensor
     * @param noSuchMetricHandler call this if the sensor raise a {@link
     *     java.util.NoSuchElementException} exception. The first argument is the identity number.
     *     The second argument is the exception itself.
     */
    public Builder addMetricSensor(
        MetricSensor metricSensor, BiConsumer<Integer, Exception> noSuchMetricHandler) {
      this.sensors.add(Map.entry(metricSensor, noSuchMetricHandler));
      return this;
    }

    /**
     * Register a {@link MetricSensor}.
     *
     * <p>This method swallow the exception caused by {@link java.util.NoSuchElementException}. For
     * further detail see {@link Builder#addMetricSensor(MetricSensor, BiConsumer)}.
     *
     * @see Builder#addMetricSensor(MetricSensor, BiConsumer)
     * @param metricSensor the sensor
     */
    public Builder addMetricSensor(MetricSensor metricSensor) {
      addMetricSensor(metricSensor, (i0, i1) -> {});
      return this;
    }

    /**
     * Add all entry to collector. Equivalent to subsequence call to {@link
     * Builder#addMetricSensor(MetricSensor, BiConsumer)}.
     *
     * @param sensors metric sensor and corresponding {@link NoSuchElementException} handler
     */
    public Builder addMetricSensors(Map<MetricSensor, BiConsumer<Integer, Exception>> sensors) {
      this.sensors.addAll(new ArrayList<>(sensors.entrySet()));
      return this;
    }

    /**
     * Add all metric sensor to collector. It is equivalent to subsequence call to {@link
     * Builder#addMetricSensor(MetricSensor)}.
     *
     * @param sensors metric sensors that we want to collect
     */
    public Builder addMetricSensors(Collection<MetricSensor> sensors) {
      sensors.forEach(this::addMetricSensor);
      return this;
    }

    public MetricCollector build() {
      // Set local mbean client as default
      this.mBeanClients.compute(
          -1,
          (id, client) -> {
            if (client != null)
              throw new IllegalArgumentException(
                  "Id conflict. Id -1 is used for local jmx connection.");
            else return MBeanClient::local;
          });
      return new LocalMetricCollector(
          threadCount,
          expiration,
          interval,
          cleanerInterval,
          this.mBeanClients.entrySet().stream()
              .map(e -> Map.entry(e.getKey(), e.getValue().get()))
              .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue)),
          this.sensors);
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
