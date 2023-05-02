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

import java.util.Collection;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.metrics.BeanQuery;
import org.astraea.common.metrics.JndiClient;
import org.astraea.common.metrics.MBeanClient;

/**
 * BeanObjectClient is a variety of MBeanClient. It stores all bean objects in memory, so it is able
 * to offer more useful information to {@link MetricSensor}. Also, this interface is used by {@link
 * MetricSensor} only, so it is free to enhance this interface without significant breaks.
 */
public interface BeanObjectClient extends MBeanClient {

  static BeanObjectClient local(int identity) {
    try (var client = JndiClient.local()) {
      return of(identity, client.beans(BeanQuery.all()));
    }
  }

  static BeanObjectClient of(int identity, Collection<BeanObject> objs) {
    return new BeanObjectClient() {

      @Override
      public int identity() {
        return identity;
      }

      @Override
      public int size() {
        return objs.size();
      }

      @Override
      public BeanObject bean(BeanQuery beanQuery) {
        return beans(beanQuery).stream()
            .findAny()
            .orElseThrow(() -> new NoSuchElementException("failed to get metrics from cache"));
      }

      @Override
      public Collection<BeanObject> beans(
          BeanQuery beanQuery, Consumer<RuntimeException> errorHandle) {
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
        return objs.stream()
            .filter(storedEntry -> wildCardDomain.matcher(storedEntry.domainName()).matches())
            .filter(
                storedEntry ->
                    wildCardProperties.entrySet().stream()
                        .allMatch(
                            e ->
                                storedEntry.properties().containsKey(e.getKey())
                                    && e.getValue()
                                        .matcher(storedEntry.properties().get(e.getKey()))
                                        .matches()))
            .collect(Collectors.toUnmodifiableList());
      }
    };
  }

  /**
   * @return identity of source
   */
  int identity();

  /**
   * @return the number of cached beans
   */
  int size();
}
