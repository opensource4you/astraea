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
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.consumer.Consumer;
import org.astraea.common.consumer.ConsumerConfigs;
import org.astraea.common.consumer.Deserializer;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.metrics.BeanQuery;
import org.astraea.common.metrics.HasBeanObject;
import org.astraea.common.metrics.MBeanClient;

public class InternalTopicCollector implements MetricCollector {
  public static final Pattern INTERNAL_TOPIC_PATTERN =
      Pattern.compile("__(?<brokerId>[0-9]+)_broker_metrics");
  private final Map<Integer, MetricStore> metricStores = new ConcurrentHashMap<>();
  private final MBeanClient local = MBeanClient.local();

  // Store those we need (we queried)
  private final Collection<Map.Entry<MetricSensor, BiConsumer<Integer, Exception>>> metricSensors;
  private final CompletableFuture<Void> future;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  private InternalTopicCollector(
      String bootstrapServer,
      Collection<Map.Entry<MetricSensor, BiConsumer<Integer, Exception>>> sensors) {
    this.metricSensors = new ArrayList<>(sensors);

    this.future =
        CompletableFuture.runAsync(
            () -> {
              try (var consumer =
                  Consumer.forTopics(INTERNAL_TOPIC_PATTERN)
                      .bootstrapServers(bootstrapServer)
                      .config(
                          ConsumerConfigs.AUTO_OFFSET_RESET_CONFIG,
                          ConsumerConfigs.AUTO_OFFSET_RESET_EARLIEST)
                      .valueDeserializer(Deserializer.BEAN_OBJECT)
                      .build()) {
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
              }
            });
  }

  @Override
  public Collection<MetricSensor> metricSensors() {
    return metricSensors.stream().map(Map.Entry::getKey).collect(Collectors.toUnmodifiableSet());
  }

  @Override
  public Set<Integer> listIdentities() {
    return Stream.concat(Stream.of(-1), metricStores.keySet().stream())
        .collect(Collectors.toUnmodifiableSet());
  }

  @Override
  public Set<Class<? extends HasBeanObject>> listMetricTypes() {
    return Stream.concat(Stream.of(Map.entry(-1, local)), metricStores.entrySet().stream())
        .flatMap(
            idClient ->
                metricSensors.stream()
                    .flatMap(
                        sensor -> {
                          try {
                            return sensor
                                .getKey()
                                .fetch(idClient.getValue(), ClusterBean.EMPTY)
                                .stream();
                          } catch (NoSuchElementException e) {
                            sensor.getValue().accept(idClient.getKey(), e);
                            return null;
                          }
                        }))
        .filter(Objects::nonNull)
        .map(bean -> bean.getClass())
        .collect(Collectors.toUnmodifiableSet());
  }

  @Override
  public int size() {
    return (int) metrics().count();
  }

  @Override
  public Stream<HasBeanObject> metrics() {
    return Stream.concat(Stream.of(Map.entry(-1, local)), metricStores.entrySet().stream())
        .flatMap(
            idClient ->
                metricSensors.stream()
                    .flatMap(
                        sensor -> {
                          try {
                            return sensor
                                .getKey()
                                .fetch(idClient.getValue(), ClusterBean.EMPTY)
                                .stream();
                          } catch (NoSuchElementException e) {
                            sensor.getValue().accept(idClient.getKey(), e);
                            return null;
                          }
                        })
                    .filter(Objects::nonNull));
  }

  @Override
  public ClusterBean clusterBean() {
    var beans =
        metricSensors.stream()
            .flatMap(
                sensor ->
                    Stream.concat(Stream.of(Map.entry(-1, local)), metricStores.entrySet().stream())
                        .flatMap(
                            idClient -> {
                              try {
                                return sensor
                                    .getKey()
                                    .fetch(idClient.getValue(), ClusterBean.EMPTY)
                                    .stream()
                                    .map(bean -> Map.entry(idClient.getKey(), bean));
                              } catch (NoSuchElementException e) {
                                sensor.getValue().accept(idClient.getKey(), e);
                                return null;
                              }
                            })
                        .filter(Objects::nonNull))
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
      future.join();
    } finally {
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
      return beans(beanQuery).stream().findAny().orElseThrow(NoSuchElementException::new);
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
      var beans =
          metricStore.entrySet().stream()
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
      if (beans.isEmpty()) throw new NoSuchElementException();
      return beans;
    }

    /** It is a fake MBeanClient. No IO resource to close. */
    @Override
    public void close() {}
  }

  static class Storage {
    private final Map<Integer, Map<BeanProperties, BeanObject>> metricStore =
        new ConcurrentHashMap<>();

    public BeanObject put(int id, BeanObject beanObject) {
      metricStore.putIfAbsent(id, new ConcurrentHashMap<>());
      return metricStore
          .get(id)
          .put(new BeanProperties(beanObject.domainName(), beanObject.properties()), beanObject);
    }

    public MBeanClient client() {
      return new MBeanClient() {
        @Override
        public BeanObject bean(BeanQuery beanQuery) {
          return null;
        }

        @Override
        public Collection<BeanObject> beans(BeanQuery beanQuery) {
          return null;
        }

        @Override
        public void close() {}
      };
    }
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
    private final List<Map.Entry<MetricSensor, BiConsumer<Integer, Exception>>> metricSensors =
        new ArrayList<>();

    public Builder bootstrapServer(String address) {
      this.bootstrapServer = address;
      return this;
    }

    public Builder addMetricSensor(MetricSensor sensor) {
      metricSensors.add(Map.entry(sensor, (i, e) -> {}));
      return this;
    }

    public Builder addMetricSensors(Collection<MetricSensor> sensors) {
      metricSensors.addAll(
          sensors.stream()
              .map(sensor -> Map.entry(sensor, (BiConsumer<Integer, Exception>) (i, e) -> {}))
              .collect(Collectors.toUnmodifiableSet()));
      return this;
    }

    public Builder addMetricSensor(MetricSensor sensor, BiConsumer<Integer, Exception> handler) {
      metricSensors.add(Map.entry(sensor, handler));
      return this;
    }

    public Builder addMetricSensors(Map<MetricSensor, BiConsumer<Integer, Exception>> sensors) {
      metricSensors.addAll(sensors.entrySet());
      return this;
    }

    public InternalTopicCollector build() {
      if (bootstrapServer.isEmpty())
        throw new IllegalArgumentException(
            "Bootstrap server is required for building InternalTopicCollector.");
      return new InternalTopicCollector(bootstrapServer, this.metricSensors);
    }
  }
}
