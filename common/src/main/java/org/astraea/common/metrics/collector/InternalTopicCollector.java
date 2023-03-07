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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.astraea.common.Utils;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.consumer.Consumer;
import org.astraea.common.consumer.ConsumerConfigs;
import org.astraea.common.consumer.Deserializer;
import org.astraea.common.consumer.SeekStrategy;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.metrics.BeanQuery;
import org.astraea.common.metrics.HasBeanObject;
import org.astraea.common.metrics.MBeanClient;

public class InternalTopicCollector implements MetricCollector {
  public static final Pattern INTERNAL_TOPIC_PATTERN =
      Pattern.compile("__(?<brokerId>[0-9]+)_broker_metrics");
  private final Map<Integer, MetricStore> metricStores = new ConcurrentHashMap<>();

  // Store those we need (we queried)
  private final Collection<MetricSensor> metricSensors;
  private final ExecutorService service;
  private final List<Consumer<byte[], BeanObject>> consumers;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  private InternalTopicCollector(
      String bootstrapServer, int threads, Collection<MetricSensor> sensors) {
    this.metricSensors = new ArrayList<>(sensors);
    this.service = Executors.newFixedThreadPool(threads);
    var tmpGroupId = Utils.randomString();
    this.consumers =
        IntStream.range(0, threads)
            .mapToObj(
                i ->
                    Consumer.forTopics(INTERNAL_TOPIC_PATTERN)
                        .bootstrapServers(bootstrapServer)
                        .config(ConsumerConfigs.GROUP_ID_CONFIG, tmpGroupId)
                        .seek(SeekStrategy.DISTANCE_FROM_BEGINNING, 0)
                        .valueDeserializer(Deserializer.BEAN_OBJECT)
                        .build())
            .collect(Collectors.toUnmodifiableList());
    this.consumers.forEach(
        consumer ->
            service.submit(
                () -> {
                  while (!closed.get()) {
                    consumer.poll(Duration.ofSeconds(1)).stream()
                        .filter(r -> r.value() != null)
                        // Parsing topic name
                        .map(r -> Map.entry(INTERNAL_TOPIC_PATTERN.matcher(r.topic()), r.value()))
                        .filter(matcherBean -> matcherBean.getKey().matches())
                        .forEach(
                            matcherBean -> {
                              int id = Integer.parseInt(matcherBean.getKey().group("brokerId"));
                              metricStores.compute(
                                  id,
                                  (ID, old) -> {
                                    if (old == null) old = new MetricStore();
                                    old.put(
                                        new BeanProperties(
                                            matcherBean.getValue().domainName(),
                                            matcherBean.getValue().properties()),
                                        matcherBean.getValue());
                                    return old;
                                  });
                            });
                  }
                }));
  }

  @Override
  public Collection<MetricSensor> metricSensors() {
    return metricSensors;
  }

  @Override
  public Set<Integer> listIdentities() {
    return metricStores.keySet();
  }

  @Override
  public Set<Class<? extends HasBeanObject>> listMetricTypes() {
    return metricStores.values().stream()
        .flatMap(
            metricStore ->
                metricSensors.stream()
                    .flatMap(sensor -> sensor.fetch(metricStore, ClusterBean.EMPTY).stream()))
        .map(bean -> bean.getClass())
        .collect(Collectors.toUnmodifiableSet());
  }

  @Override
  public int size() {
    return metricStores.values().stream()
        .map(metricStore -> metricStore.beans(BeanQuery.all()))
        .mapToInt(Collection::size)
        .sum();
  }

  @Override
  public Stream<HasBeanObject> metrics() {
    return metricStores.values().stream()
        .flatMap(
            store ->
                metricSensors.stream()
                    .flatMap(sensor -> sensor.fetch(store, ClusterBean.EMPTY).stream()));
  }

  @Override
  public ClusterBean clusterBean() {
    var beans =
        metricSensors.stream()
            .flatMap(
                sensor ->
                    metricStores.entrySet().stream()
                        .flatMap(
                            idStore ->
                                sensor.fetch(idStore.getValue(), ClusterBean.EMPTY).stream()
                                    .map(bean -> Map.entry(idStore.getKey(), bean))))
            .collect(
                Collectors.groupingBy(
                    Map.Entry::getKey,
                    HashMap::new,
                    Collectors.mapping(Map.Entry::getValue, Collectors.toUnmodifiableSet())));
    return ClusterBean.of(beans);
  }

  @Override
  public void close() {
    try {
      this.closed.set(true);
      service.shutdown();
      service.awaitTermination(10, TimeUnit.SECONDS);
    } catch (InterruptedException ie) {
      ie.printStackTrace();
    } finally {
      this.consumers.forEach(Consumer::close);
      this.metricStores.values().forEach(MetricStore::close);
    }
  }

  /**
   * An object for storing mbeans and for querying. MetricStore cache the latest value for each type
   * of metrics. Metric type is classified by its domain name (String) and properties (Map<String,
   * String>). Fake MBean client that filter out the queried beans. This class did not create
   * connection. This is used to pass to metricSensor to query beans.
   */
  static class MetricStore implements MBeanClient {
    private final Map<BeanProperties, BeanObject> metricStore = new ConcurrentHashMap<>();

    /** Update BeanObjects stored locally. */
    BeanObject put(BeanProperties beanProperties, BeanObject beanObject) {
      return metricStore.put(beanProperties, beanObject);
    }

    /**
     * Return any beanObject that match the given beanQuery.
     *
     * @param beanQuery the non-pattern BeanQuery
     * @return A {@link BeanObject} contain given specific attributes if target resolved
     *     successfully.
     */
    @Override
    public BeanObject bean(BeanQuery beanQuery) {
      return beans(beanQuery).stream().findAny().orElse(null);
    }

    @Override
    public Collection<BeanObject> beans(BeanQuery beanQuery) {
      // The queried domain name (or properties) may contain wildcard. Change wildcard to regular
      // expression.
      var wildCardDomain =
          Pattern.compile(beanQuery.domainName().replaceAll("[*]", ".*").replaceAll("[?]", "."));
      var wildCardProperties =
          beanQuery.properties().entrySet().stream()
              .collect(
                  Collectors.toUnmodifiableMap(
                      Map.Entry::getKey,
                      e ->
                          Pattern.compile(
                              e.getValue().replaceAll("[*]", ".*").replaceAll("[?]", "."))));
      // Filtering out beanObject that match the query
      return metricStore.entrySet().stream()
          .filter(storedEntry -> wildCardDomain.matcher(storedEntry.getKey().domain).matches())
          .filter(
              storedEntry ->
                  wildCardProperties.entrySet().stream()
                      .allMatch(
                          e ->
                              storedEntry.getKey().properties.containsKey(e.getKey())
                                  && e.getValue()
                                      .matcher(storedEntry.getKey().properties.get(e.getKey()))
                                      .matches()))
          .map(Map.Entry::getValue)
          .collect(Collectors.toUnmodifiableSet());
    }

    /** It is a fake MBeanClient. No IO resource to close. */
    @Override
    public void close() {}
  }

  static class BeanProperties {
    private final String domain;
    private final Map<String, String> properties;

    public BeanProperties(String domain, Map<String, String> properties) {
      this.domain = domain;
      this.properties = properties;
    }

    @Override
    public int hashCode() {
      return domain.hashCode() ^ properties.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (!(obj instanceof BeanProperties)) return false;
      if (!this.domain.equals(((BeanProperties) obj).domain)) return false;
      return this.properties.equals(((BeanProperties) obj).properties);
    }
  }

  public static class Builder {
    private String bootstrapServer = "";
    private int threads = 1;
    private List<MetricSensor> metricSensors = new ArrayList<>();

    public Builder bootstrapServer(String address) {
      this.bootstrapServer = address;
      return this;
    }

    public Builder threads(int threads) {
      this.threads = threads;
      return this;
    }

    public Builder addMetricSensor(MetricSensor sensor) {
      metricSensors.add(sensor);
      return this;
    }

    public Builder addMetricSensors(Collection<MetricSensor> sensors) {
      metricSensors.addAll(sensors);
      return this;
    }

    public InternalTopicCollector build() {
      if (bootstrapServer.isEmpty())
        throw new IllegalArgumentException(
            "Bootstrap server is required for building InternalTopicCollector.");
      return new InternalTopicCollector(bootstrapServer, threads, this.metricSensors);
    }
  }
}
