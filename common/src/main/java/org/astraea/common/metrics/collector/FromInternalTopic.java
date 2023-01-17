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

import static org.astraea.common.consumer.SeekStrategy.DISTANCE_FROM_BEGINNING;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.consumer.Consumer;
import org.astraea.common.consumer.Deserializer;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.metrics.BeanQuery;
import org.astraea.common.metrics.HasBeanObject;
import org.astraea.common.metrics.MBeanClient;

public class FromInternalTopic implements MetricCollector {
  public static final Pattern INTERNAL_TOPIC_PATTERN =
      Pattern.compile("__(?<brokerId>[0-9]+)_broker_metrics");

  // broker id and corresponding fake mbean client
  private final Map<Integer, MetricStore> metricStores = new ConcurrentHashMap<>();

  private final AtomicBoolean isClose = new AtomicBoolean(false);

  private final CompletableFuture<Void> collector;

  private final List<Fetcher> fetchers = new ArrayList<>();

  public FromInternalTopic(String bootstrapServer) {

    // Keep fetching
    collector =
        CompletableFuture.runAsync(
            () -> {
              try (var consumer =
                  Consumer.forTopics(INTERNAL_TOPIC_PATTERN)
                      .bootstrapServers(bootstrapServer)
                      .keyDeserializer(Deserializer.STRING)
                      .valueDeserializer(
                          (topic, headers, data) ->
                              stringToBean(Deserializer.STRING.deserialize(topic, headers, data)))
                      .seek(DISTANCE_FROM_BEGINNING, 0)
                      .build()) {
                while (!isClose.get()) {
                  var records = consumer.poll(Duration.ofSeconds(20));
                  records.stream()
                      .filter(r -> r.value() != null)
                      // Parsing topic name
                      .map(r -> Map.entry(INTERNAL_TOPIC_PATTERN.matcher(r.topic()), r.value()))
                      .filter(topicAndBean -> topicAndBean.getKey().matches())
                      .map(
                          topicAndBean ->
                              Map.entry(
                                  topicAndBean.getKey().group("brokerId"), topicAndBean.getValue()))
                      .forEach(
                          idAndBean -> {
                            int id = Integer.parseInt(idAndBean.getKey());
                            metricStores.putIfAbsent(id, new MetricStore());
                            metricStores
                                .get(id)
                                .put(
                                    new BeanProperties(
                                        idAndBean.getValue().domainName(),
                                        idAndBean.getValue().properties()),
                                    idAndBean.getValue());
                          });
                }
              }
            });
  }

  @Override
  public void addFetcher(Fetcher fetcher, BiConsumer<Integer, Exception> noSuchMetricHandler) {
    fetchers.add(fetcher);
  }

  @Override
  public void addMetricSensors(MetricSensor metricSensor) {}

  @Override
  public void registerJmx(int identity, InetSocketAddress socketAddress) {}

  @Override
  public void registerLocalJmx(int identity) {}

  @Override
  public Collection<MetricSensor> listMetricsSensors() {
    return null;
  }

  @Override
  public Collection<Fetcher> listFetchers() {
    return fetchers;
  }

  @Override
  public Set<Integer> listIdentities() {
    return metricStores.keySet();
  }

  @Override
  public Set<Class<? extends HasBeanObject>> listMetricTypes() {
    return null;
  }

  @Override
  public int size() {
    return 0;
  }

  @Override
  public Stream<HasBeanObject> metrics() {
    return null;
  }

  @Override
  public ClusterBean clusterBean() {
    var beans =
        fetchers.stream()
            .flatMap(
                f ->
                    metricStores.entrySet().stream()
                        .map(
                            idStore ->
                                Map.entry(
                                    idStore.getKey(),
                                    (Collection<HasBeanObject>) f.fetch(idStore.getValue()))))
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
    return ClusterBean.of(beans);
  }

  @Override
  public void close() {
    isClose.set(true);
    collector.cancel(true);
  }

  public static BeanObject stringToBean(String beanString) {
    Pattern p =
        Pattern.compile("\\[(?<domain>[^:]*):(?<properties>[^]]*)]\n\\{(?<attributes>[^}]*)}");
    Matcher m = p.matcher(beanString);
    if (!m.matches()) {
      return null;
    }
    var domain = m.group("domain");
    var propertiesPairs = m.group("properties").split("[, ]");
    var attributesPairs = m.group("attributes").split("[, ]");
    var properties =
        Arrays.stream(propertiesPairs)
            .filter(kv -> kv.split("=").length >= 2)
            .collect(Collectors.toUnmodifiableMap(kv -> kv.split("=")[0], kv -> kv.split("=")[1]));
    var attributes =
        Arrays.stream(attributesPairs)
            .filter(kv -> kv.split("=").length >= 2)
            .collect(
                Collectors.toUnmodifiableMap(
                    kv -> kv.split("=")[0], kv -> (Object) kv.split("=")[1]));
    return new BeanObject(domain, properties, attributes);
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

  /**
   * An object for storing mbeans and for querying. MetricStore cache the latest value for each type
   * of metrics. Metric type is classified by its domain name (String) and properties (Map<String,
   * String>). Fake MBean client that filter out the queried beans. This class did not create
   * connection. This is used to pass to fetchers to query beans.
   */
  static class MetricStore implements MBeanClient {
    private final Map<BeanProperties, BeanObject> metricStore = new ConcurrentHashMap<>();

    public BeanObject put(BeanProperties beanProperties, BeanObject beanObject) {
      return metricStore.put(beanProperties, beanObject);
    }

    @Override
    public BeanObject queryBean(BeanQuery beanQuery) {
      return queryBean(beanQuery, List.of());
    }

    /**
     * Always return beanObject with all attribute.
     *
     * <p>Same result of using {@link #queryBeans(BeanQuery)} for this implementation. This
     * implementation filter beanObjects by BeanQuery only.
     *
     * @param beanQuery the non-pattern BeanQuery
     * @param attributeNameCollection no use
     * @return A {@link BeanObject} contain given specific attributes if target resolved
     *     successfully.
     */
    @Override
    public BeanObject queryBean(BeanQuery beanQuery, Collection<String> attributeNameCollection) {
      return queryBeans(beanQuery).stream().findAny().orElse(null);
    }

    @Override
    public Collection<BeanObject> queryBeans(BeanQuery beanQuery) {
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

    /** Meaningless for this implementation */
    @Override
    public List<String> listDomains() {
      return null;
    }

    @Override
    public String host() {
      return null;
    }

    @Override
    public int port() {
      return 0;
    }

    @Override
    public void close() {}
  }
}
