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
package org.astraea.common.metrics;

import java.util.Collection;
import java.util.Set;
import java.util.function.Consumer;
import javax.management.ObjectName;

public interface MBeanClient {

  /**
   * Fetch all attributes of target mbean.
   *
   * <p>Note that when exception is raised during the attribute fetching process, the exact
   * exception will be placed into the attribute field.
   *
   * @param beanQuery the non-pattern BeanQuery
   * @return A {@link BeanObject} contain all attributes if target resolved successfully.
   */
  BeanObject bean(BeanQuery beanQuery);

  default Collection<BeanObject> beans(BeanQuery beanQuery) {
    return beans(
        beanQuery,
        e -> {
          throw e;
        });
  }

  /**
   * Query mBeans by pattern.
   *
   * <p>Query mbeans by {@link ObjectName} pattern, the returned {@link BeanObject}s will contain
   * all the available attributes
   *
   * <p>Note that when exception is raised during the attribute fetching process, the exact
   * exception will be placed into the attribute field.
   *
   * @param beanQuery the pattern to query
   * @param errorHandle used to handle the error when fetching specify bean from remote server
   * @return A {@link Set} of {@link BeanObject}, all BeanObject has its own attributes resolved.
   */
  Collection<BeanObject> beans(BeanQuery beanQuery, Consumer<RuntimeException> errorHandle);
}
