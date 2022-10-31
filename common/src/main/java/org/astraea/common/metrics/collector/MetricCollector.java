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
import java.util.Collection;
import java.util.Map;
import org.astraea.common.metrics.HasBeanObject;

public interface MetricCollector extends AutoCloseable {

  /** Register a {@link Fetcher}. */
  void addFetcher(Fetcher fetcher);

  /** Register a JMX server. */
  void registerJmx(int identity, InetSocketAddress socketAddress);

  /**
   * Retrieve metrics with specific class from all brokers.
   *
   * @return a readonly view of underlying storage map
   */
  <T extends HasBeanObject> Map<Integer, Collection<T>> metrics(Class<T> metricClass);

  @Override
  void close();

  static MetricCollectorImpl.Builder builder() {
    return new MetricCollectorImpl.Builder();
  }
}
