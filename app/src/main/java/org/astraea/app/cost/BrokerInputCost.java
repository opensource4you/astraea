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
package org.astraea.app.cost;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.app.metrics.collector.Fetcher;
import org.astraea.app.metrics.kafka.BrokerTopicMetricsResult;
import org.astraea.app.metrics.kafka.KafkaMetrics;

/**
 * The result is computed by "BytesInPerSec.count". "BytesInPerSec.count" responds to the input
 * throughput of brokers.
 *
 * <ol>
 *   <li>We normalize the metric as score(by T-score).
 *   <li>We record these data of each second.
 *   <li>We only keep the last ten seconds of data.
 *   <li>The final result is the average of the ten-second data.
 * </ol>
 */
public class BrokerInputCost implements HasBrokerCost {
  private final Map<Integer, BrokerMetric> brokersMetric = new HashMap<>();

  @Override
  public BrokerCost brokerCost(ClusterInfo clusterInfo) {
    var costMetrics =
        clusterInfo.clusterBean().all().entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    entry ->
                        entry.getValue().stream()
                            .filter(
                                hasBeanObject ->
                                    KafkaMetrics.BrokerTopic.BytesInPerSec.metricName()
                                        .equals(
                                            hasBeanObject.beanObject().getProperties().get("name")))
                            .findAny()
                            .orElseThrow()))
            .entrySet()
            .stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    entry -> {
                      if (!brokersMetric.containsKey(entry.getKey())) {
                        brokersMetric.put(entry.getKey(), new BrokerMetric());
                      }
                      var broker = brokersMetric.get(entry.getKey());
                      var inBean = (BrokerTopicMetricsResult) entry.getValue();
                      var count = (double) (inBean.count() - broker.accumulateCount);
                      broker.accumulateCount = inBean.count();
                      return count;
                    }));
    costMetrics.forEach((broker, v) -> brokersMetric.get(broker).updateLoad(v));

    return this::computeLoad;
  }

  Map<Integer, Double> computeLoad() {
    return brokersMetric.entrySet().stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                e ->
                    Math.round(
                            e.getValue().load.stream()
                                    .filter(aDouble -> !aDouble.equals(-1.0))
                                    .mapToDouble(i -> i)
                                    .average()
                                    .orElse(0.5)
                                * 100)
                        / 100.0));
  }

  @Override
  public Optional<Fetcher> fetcher() {
    return Optional.of(client -> List.of(KafkaMetrics.BrokerTopic.BytesInPerSec.fetch(client)));
  }

  private static class BrokerMetric {
    // mbean data.  BytesInPerSec.count
    private long accumulateCount = 0L;

    // Record the latest 10 numbers only.
    private final List<Double> load =
        IntStream.range(0, 10).mapToObj(i -> -1.0).collect(Collectors.toList());
    private int loadIndex = 0;

    private BrokerMetric() {}

    /**
     * This method record input data into a list. This list contains the latest 10 record. Each time
     * it is called, the current index, "loadIndex", is increased by 1.
     */
    void updateLoad(Double load) {
      var cLoad = load.isNaN() ? 0.5 : load;
      this.load.set(loadIndex, cLoad);
      loadIndex = (loadIndex + 1) % 10;
    }
  }
}
