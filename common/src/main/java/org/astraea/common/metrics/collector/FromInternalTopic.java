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
import java.util.Arrays;
import java.util.Collection;
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
import org.astraea.common.metrics.HasBeanObject;

public class FromInternalTopic implements MetricCollector {
  public static final Pattern INTERNAL_TOPIC_PATTERN =
      Pattern.compile("__(?<brokerId>[0-9]+)_broker_metrics");

  /* Map<brokerId, Map<domain+properties, bean>>
   * metricStore variable will cache the latest value for each type of metrics.
   * Metric type is classified by its {domain name (String) + properties (Map<String, String>)}
   * */
  private final Map<Integer, Map<String, HasBeanObject>> metricStore = new ConcurrentHashMap<>();

  private final AtomicBoolean isClose = new AtomicBoolean(false);

  private CompletableFuture<Void> collector;

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
                            metricStore.putIfAbsent(id, new ConcurrentHashMap<>());
                            metricStore
                                .get(id)
                                .put(
                                    idAndBean.getValue().beanObject().domainName()
                                        + idAndBean.getValue().beanObject().properties(),
                                    idAndBean.getValue());
                          });
                }
              }
            });
  }

  @Override
  public void addFetcher(Fetcher fetcher, BiConsumer<Integer, Exception> noSuchMetricHandler) {}

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
    return null;
  }

  @Override
  public Set<Integer> listIdentities() {
    return null;
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
    return ClusterBean.of(
        metricStore.entrySet().stream()
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, e -> e.getValue().values())));
  }

  @Override
  public void close() {
    isClose.set(true);
    collector.cancel(true);
  }

  public static HasBeanObject stringToBean(String beanString) {
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
    return () -> new BeanObject(domain, properties, attributes);
  }
}
