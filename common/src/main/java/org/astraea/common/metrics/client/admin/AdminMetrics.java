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
package org.astraea.common.metrics.client.admin;

import java.util.Collection;
import org.astraea.common.Utils;
import org.astraea.common.metrics.BeanQuery;
import org.astraea.common.metrics.MBeanClient;
import org.astraea.common.metrics.client.HasNodeMetrics;
import org.astraea.common.metrics.client.HasSelectorMetrics;

public class AdminMetrics {

  public static final BeanQuery NODE_QUERY =
      BeanQuery.builder()
          .domainName("kafka.admin.client")
          .property("type", "admin-client-node-metrics")
          .property("node-id", "*")
          .property("client-id", "*")
          .build();

  public static final BeanQuery ADMIN_QUERY =
      BeanQuery.builder()
          .domainName("kafka.admin.client")
          .property("type", "admin-client-metrics")
          .property("client-id", "*")
          .build();

  public static final Collection<BeanQuery> QUERIES =
      Utils.constants(AdminMetrics.class, name -> name.endsWith("QUERY"), BeanQuery.class);

  /**
   * collect HasNodeMetrics from all consumers.
   *
   * @param mBeanClient to query metrics
   * @return key is broker id, and value is associated to broker metrics recorded by all consumers
   */
  public static Collection<HasNodeMetrics> node(MBeanClient mBeanClient) {
    return mBeanClient.beans(NODE_QUERY).stream().map(b -> (HasNodeMetrics) () -> b).toList();
  }

  public static Collection<HasSelectorMetrics> admin(MBeanClient mBeanClient) {
    return mBeanClient.beans(ADMIN_QUERY).stream().map(b -> (HasSelectorMetrics) () -> b).toList();
  }
}
