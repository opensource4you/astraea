package org.astraea.balancer.alpha;

import java.time.Duration;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.management.remote.JMXServiceURL;
import org.astraea.Utils;
import org.astraea.metrics.HasBeanObject;
import org.astraea.metrics.collector.Fetcher;
import org.astraea.metrics.jmx.MBeanClient;

/** Doing metric collector for balancer. */
public class MetricCollector implements AutoCloseable {

  private final long fetchInterval = Duration.ofSeconds(1).toMillis();
  /**
   * The number of old time series to keep in data structure. note that this is not a strict upper
   * limit. The actual size might exceed. This issue is minor and fixing that might cause
   * performance issue. So no. This number must be larger than zero.
   */
  private final int timeSeriesKeeps = 5000;

  private final Map<Integer, MBeanClient> mBeanClientMap;
  private final Map<Integer, JMXServiceURL> jmxServiceURLMap;
  private final Fetcher aggregatedFetcher;
  private final Map<Integer, ConcurrentLinkedQueue<HasBeanObject>> metricTimeSeries;
  private final ScheduledExecutorService executor;
  private final AtomicBoolean started = new AtomicBoolean();
  private final List<ScheduledFuture<?>> scheduledFutures = new LinkedList<>();

  /**
   * BalancerMetricCollector
   *
   * @param jmxServiceURLMap the map of brokerId and corresponding JmxUrl
   * @param fetchers the fetcher of interested metrics
   * @param scheduledExecutorService the executor service for schedule tasks, <strong>DO NOT
   *     SHUTDOWN THIS</strong>.
   */
  public MetricCollector(
      Map<Integer, JMXServiceURL> jmxServiceURLMap,
      Collection<Fetcher> fetchers,
      ScheduledExecutorService scheduledExecutorService) {
    this.jmxServiceURLMap = Map.copyOf(jmxServiceURLMap);
    this.aggregatedFetcher = Fetcher.of(fetchers);
    this.mBeanClientMap = new ConcurrentHashMap<>();
    this.metricTimeSeries = new ConcurrentHashMap<>();
    this.executor = scheduledExecutorService;
  }

  public synchronized void start() {
    if (!started.compareAndSet(false, true)) {
      throw new IllegalStateException("This metric collector already started");
    }

    jmxServiceURLMap.forEach(
        (brokerId, serviceUrl) -> mBeanClientMap.put(brokerId, MBeanClient.of(serviceUrl)));

    Consumer<Integer> task =
        (brokerId) -> {
          // the following code section perform multiple modification on this data structure without
          // atomic guarantee. this is done by the thread confinement technique. So for any time
          // moment, only one thread can be the writer to this data structure.
          metricTimeSeries
              .computeIfAbsent(brokerId, (ignore) -> new ConcurrentLinkedQueue<>())
              .addAll(aggregatedFetcher.fetch(mBeanClientMap.get(brokerId)));
          while (metricTimeSeries.get(brokerId).size() > timeSeriesKeeps)
            metricTimeSeries.get(brokerId).poll();
        };

    // schedule the fetching process for every broker.
    var futures =
        mBeanClientMap.keySet().stream()
            .map(
                brokerId ->
                    executor.scheduleAtFixedRate(
                        () -> task.accept(brokerId), 0, fetchInterval, TimeUnit.MILLISECONDS))
            .collect(Collectors.toUnmodifiableList());
    scheduledFutures.addAll(futures);
  }

  /**
   * fetch metrics from the specific brokers. This method is thread safe.
   *
   * @param brokerId the broker id
   * @return a list of requested metrics.
   */
  public synchronized List<HasBeanObject> fetchBrokerMetrics(Integer brokerId) {
    // concurrent data structure + thread confinement to one writer + immutable objects
    if (!started.get()) throw new IllegalStateException("This MetricCollector haven't started");
    return List.copyOf(
        metricTimeSeries.computeIfAbsent(brokerId, (ignore) -> new ConcurrentLinkedQueue<>()));
  }

  public synchronized Map<Integer, Collection<HasBeanObject>> fetchMetrics() {
    return this.jmxServiceURLMap.keySet().stream()
        .collect(Collectors.toUnmodifiableMap(Function.identity(), this::fetchBrokerMetrics));
  }

  @Override
  public synchronized void close() {
    if (!started.get()) return;
    started.set(false);
    scheduledFutures.forEach(x -> x.cancel(true));
    scheduledFutures.forEach(x -> Utils.waitFor(x::isCancelled));
    mBeanClientMap.values().forEach(MBeanClient::close);
    mBeanClientMap.clear();
    metricTimeSeries.clear();
  }
}
