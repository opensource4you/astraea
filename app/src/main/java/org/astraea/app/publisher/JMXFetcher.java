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
package org.astraea.app.publisher;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.metrics.BeanQuery;
import org.astraea.common.metrics.MBeanClient;

/** A pool of threads keep fetching MBeans of target brokerIds. */
public interface JMXFetcher extends AutoCloseable {
  static JMXFetcher create(String address, int port) {
    var client = MBeanClient.jndi(address, port);
    // Query all beans by default
    var queryList = new ArrayList<>(List.of(BeanQuery.all()));
    return new JMXFetcher() {
      @Override
      public Collection<BeanObject> fetch() {
        return queryList.stream()
            .map(client::beans)
            .flatMap(Collection::stream)
            .collect(Collectors.toList());
      }

      @Override
      public void addQuery(BeanQuery beanQuery) {
        queryList.add(beanQuery);
      }

      @Override
      public void clearQuery() {
        queryList.clear();
      }

      @Override
      public void close() {
        client.close();
      }
    };
  }

  Collection<BeanObject> fetch();

  void addQuery(BeanQuery beanQuery);

  void clearQuery();

  void close();

  class IdAndBean {
    private final int id;
    private final BeanObject bean;

    public IdAndBean(int id, BeanObject bean) {
      this.id = id;
      this.bean = bean;
    }

    public int id() {
      return this.id;
    }

    public BeanObject bean() {
      return bean;
    }
  }
}
