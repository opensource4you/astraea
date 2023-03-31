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
package org.astraea.common.metrics.platform;

import java.util.Collection;
import org.astraea.common.Utils;
import org.astraea.common.metrics.BeanQuery;
import org.astraea.common.metrics.MBeanClient;

public final class HostMetrics {
  public static final BeanQuery OPERATING_SYSTEM_QUERY =
      BeanQuery.builder().domainName("java.lang").property("type", "OperatingSystem").build();
  public static final BeanQuery JVM_MEMORY_QUERY =
      BeanQuery.builder().domainName("java.lang").property("type", "Memory").build();

  public static final Collection<BeanQuery> QUERIES =
      Utils.constants(HostMetrics.class, name -> name.endsWith("QUERY"), BeanQuery.class);

  public static OperatingSystemInfo operatingSystem(MBeanClient mBeanClient) {
    return new OperatingSystemInfo(mBeanClient.bean(OPERATING_SYSTEM_QUERY));
  }

  public static JvmMemory jvmMemory(MBeanClient mBeanClient) {
    return new JvmMemory(mBeanClient.bean(JVM_MEMORY_QUERY));
  }

  private HostMetrics() {}
}
