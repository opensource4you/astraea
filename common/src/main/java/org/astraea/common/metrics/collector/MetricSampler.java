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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.astraea.common.Utils;
import org.astraea.common.admin.ClusterBean;

public class MetricSampler implements AutoCloseable {

  private final BeanCollector beanCollector;
  private final Map<Integer, Receiver> receivers;
  private final ScheduledExecutorService executor;
  private final List<ScheduledFuture<?>> scheduledFutures;
  private final Duration metricSampleInterval;

  public MetricSampler(Fetcher fetcher, Map<Integer, InetSocketAddress> brokerJmxAddresses) {
    this(fetcher, Duration.ofSeconds(1), brokerJmxAddresses);
  }

  public MetricSampler(
      Fetcher fetcher,
      Duration sampleInterval,
      Map<Integer, InetSocketAddress> brokerJmxAddresses) {
    this.metricSampleInterval = sampleInterval;
    var theFetcher = Optional.ofNullable(fetcher);
    this.beanCollector =
        BeanCollector.builder().numberOfObjectsPerNode(2000).interval(metricSampleInterval).build();
    // if no fetcher is supplied, no receiver will be created.
    this.receivers =
        theFetcher
            .map(
                fetch ->
                    brokerJmxAddresses.entrySet().stream()
                        .collect(
                            Collectors.toUnmodifiableMap(
                                Map.Entry::getKey,
                                entry ->
                                    beanCollector
                                        .register()
                                        .host(entry.getValue().getHostName())
                                        .port(entry.getValue().getPort())
                                        .fetcher(fetch)
                                        .build())))
            .orElse(Map.of());
    // the number of threads for sampling, limited by the process count
    int threadCount = Math.min(Runtime.getRuntime().availableProcessors(), receivers.size());
    this.executor = Executors.newScheduledThreadPool(threadCount);
    this.scheduledFutures =
        receivers.values().stream()
            .map(
                receiver ->
                    executor.scheduleAtFixedRate(
                        receiver::current,
                        metricSampleInterval.toMillis(),
                        metricSampleInterval.toMillis(),
                        TimeUnit.MILLISECONDS))
            .collect(Collectors.toUnmodifiableList());
  }

  public ClusterBean offer() {
    if (receivers.isEmpty()) return ClusterBean.EMPTY;
    return ClusterBean.of(
        receivers.entrySet().stream()
            .collect(
                Collectors.toUnmodifiableMap(
                    Map.Entry::getKey, e -> List.copyOf(e.getValue().current()))));
  }

  public Duration interval() {
    return metricSampleInterval;
  }

  @Override
  public void close() {
    this.scheduledFutures.forEach(x -> x.cancel(false));
    this.executor.shutdown();
    Utils.packException(() -> this.executor.awaitTermination(10, TimeUnit.SECONDS));
    receivers.values().forEach(Receiver::close);
  }
}
