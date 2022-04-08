package org.astraea.balancer.alpha;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.management.remote.JMXServiceURL;
import org.astraea.Utils;
import org.astraea.metrics.HasBeanObject;
import org.astraea.metrics.collector.Fetcher;
import org.astraea.metrics.jmx.MBeanClient;
import org.astraea.metrics.kafka.KafkaMetrics;

/** Doing metric collector for balancer. */
public class MetricCollector implements AutoCloseable {

  private final long fetchInterval = Duration.ofSeconds(1).toMillis();
  /**
   * The number of old time series to keep in data structure. note that this is not a strict upper
   * limit. The actual size might exceed. This issue is minor and fixing that might cause
   * performance issue. So no. This number must be larger than zero.
   */
  private final int timeSeriesKeeps = 5;

  private final Map<Integer, MBeanClient> mBeanClientMap;
  private final Map<Integer, JMXServiceURL> jmxServiceURLMap;
  private final List<Fetcher> fetchers;
  private final Map<
          Integer, Map<Fetcher, ConcurrentLinkedQueue<TimeSeries<Collection<HasBeanObject>>>>>
      metricTimeSeries;
  private final ScheduledExecutorService executor;
  private final AtomicBoolean started = new AtomicBoolean();
  private final AtomicBoolean isStopped = new AtomicBoolean();
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
    this.fetchers = List.copyOf(fetchers);
    this.mBeanClientMap = new ConcurrentHashMap<>();
    this.metricTimeSeries = new ConcurrentHashMap<>();
    jmxServiceURLMap
        .keySet()
        .forEach(
            (brokerId) -> {
              var map =
                  new ConcurrentHashMap<
                      Fetcher, ConcurrentLinkedQueue<TimeSeries<Collection<HasBeanObject>>>>();
              fetchers.forEach(fetcher -> map.put(fetcher, new ConcurrentLinkedQueue<>()));
              metricTimeSeries.put(brokerId, map);
            });
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
          fetchers.stream()
              .map(x -> Map.entry(x, x.fetch(mBeanClientMap.get(brokerId))))
              .forEach(
                  (entry) -> {
                    var fetcher = entry.getKey();
                    var metrics = entry.getValue();

                    // the following code perform thread confinement to excess avoid locking
                    // (the fetching operation of every broker can only be executed by one thread at
                    // a time)
                    metricTimeSeries.get(brokerId).get(fetcher).add(TimeSeries.ofNow(metrics));

                    // try to limit the amount of objects in this data structure.
                    // It doesn't matter if client will see a few more objects, so no atomic
                    // operation here.
                    int size = metricTimeSeries.get(brokerId).get(fetcher).size();
                    if (size > timeSeriesKeeps) {
                      int objectsToRemove = size - timeSeriesKeeps;
                      // this thread should be the only writer at this moment, right?
                      assert objectsToRemove == 1;
                      for (int i = 0; i < objectsToRemove; i++)
                        metricTimeSeries.get(brokerId).get(fetcher).poll();
                    }
                  });
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
   * @param fetcher the fetcher thread
   * @return a list of requested metrics.
   */
  public List<TimeSeries<Collection<HasBeanObject>>> fetchBrokerMetrics(
      Integer brokerId, Fetcher fetcher) {
    return metricTimeSeries.get(brokerId).get(fetcher).stream()
        .collect(Collectors.toUnmodifiableList());
  }

  @Override
  public synchronized void close() {
    if (!started.get()) return;
    if (isStopped.get()) return;
    isStopped.set(true);
    scheduledFutures.forEach(x -> x.cancel(true));
    scheduledFutures.forEach(x -> Utils.waitFor(x::isCancelled));
    mBeanClientMap.values().forEach(MBeanClient::close);
    mBeanClientMap.clear();
    metricTimeSeries.clear();
  }

  public static class TimeSeries<T> {
    public final LocalDateTime time;
    public final T data;

    private TimeSeries(LocalDateTime time, T data) {
      this.time = time;
      this.data = data;
    }

    static <T> TimeSeries<T> of(LocalDateTime time, T data) {
      return new TimeSeries<>(time, data);
    }

    static <T> TimeSeries<T> ofNow(T data) {
      return of(LocalDateTime.now(), data);
    }
  }

  public static void main(String[] args) throws InterruptedException {

    var jmx =
        (BiFunction<String, String, JMXServiceURL>)
            (host, port) ->
                Utils.handleException(
                    () ->
                        new JMXServiceURL(
                            String.format(
                                "service:jmx:rmi://%s:%s/jndi/rmi://%s:%s/jmxrmi",
                                host, port, host, port)));

    var map = Map.of(1001, jmx.apply("192.168.103.49", "10528"));
    var fetchers =
        List.<Fetcher>of(
            (c) -> List.of(KafkaMetrics.BrokerTopic.BytesInPerSec.fetch(c)),
            (c) -> List.of(KafkaMetrics.BrokerTopic.BytesOutPerSec.fetch(c)));

    final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(8);

    final MetricCollector balancerMetricCollector =
        new MetricCollector(map, fetchers, scheduledExecutorService);

    balancerMetricCollector.start();

    for (int i = 0; i < 1000; i++) {
      TimeUnit.SECONDS.sleep(1);
      System.out.println(LocalDateTime.now());
      balancerMetricCollector
          .fetchBrokerMetrics(1001, fetchers.get(0))
          .forEach(
              metric -> {
                System.out.printf(
                    "%s: %s%n",
                    metric.time,
                    metric.data.stream().findFirst().get().toString().replace("\n", " "));
              });
      balancerMetricCollector
          .fetchBrokerMetrics(1001, fetchers.get(1))
          .forEach(
              metric -> {
                System.out.printf(
                    "%s: %s%n",
                    metric.time,
                    metric.data.stream().findFirst().get().toString().replace("\n", " "));
              });
    }

    balancerMetricCollector.close();
  }
}
