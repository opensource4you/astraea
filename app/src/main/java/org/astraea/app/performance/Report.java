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
package org.astraea.app.performance;

import java.util.List;
import java.util.stream.Collectors;
import org.astraea.common.metrics.MBeanClient;
import org.astraea.common.metrics.client.consumer.ConsumerMetrics;
import org.astraea.common.metrics.client.consumer.HasConsumerFetchMetrics;
import org.astraea.common.metrics.client.producer.ProducerMetrics;

public interface Report {

  static long recordsConsumedTotal() {
    var client = MBeanClient.local();
    return (long)
        ConsumerMetrics.fetches(client).stream()
            .mapToDouble(HasConsumerFetchMetrics::recordsConsumedTotal)
            .sum();
  }

  static List<Report> consumers() {

    return ConsumerMetrics.fetches(MBeanClient.local()).stream()
        .map(
            m ->
                new Report() {
                  @Override
                  public long records() {
                    return (long) m.recordsConsumedTotal();
                  }

                  @Override
                  public long maxLatency() {
                    return (long) m.fetchLatencyMax();
                  }

                  @Override
                  public double avgLatency() {
                    return m.fetchLatencyAvg();
                  }

                  @Override
                  public long totalBytes() {
                    return (long) m.bytesConsumedTotal();
                  }

                  @Override
                  public double avgThroughput() {
                    return m.bytesConsumedRate();
                  }

                  @Override
                  public String clientId() {
                    return m.clientId();
                  }
                })
        .collect(Collectors.toList());
  }

  static List<Report> producers() {
    return ProducerMetrics.of(MBeanClient.local()).stream()
        .map(
            m ->
                new Report() {
                  @Override
                  public long records() {
                    return (long) m.recordSendTotal();
                  }

                  @Override
                  public long maxLatency() {
                    return (long) m.requestLatencyMax();
                  }

                  @Override
                  public double avgLatency() {
                    return m.requestLatencyAvg();
                  }

                  @Override
                  public long totalBytes() {
                    return (long) m.outgoingByteTotal();
                  }

                  @Override
                  public double avgThroughput() {
                    return m.outgoingByteRate();
                  }

                  @Override
                  public String clientId() {
                    return m.clientId();
                  }
                })
        .collect(Collectors.toList());
  }

  /**
   * @return Get the number of records.
   */
  long records();
  /**
   * @return Get the maximum of latency put.
   */
  long maxLatency();

  /**
   * @return Get the average latency.
   */
  double avgLatency();

  /**
   * @return total send/received bytes
   */
  long totalBytes();

  double avgThroughput();

  String clientId();
}
