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
package org.astraea.app.metrics.collector;

import java.util.Collection;
import org.astraea.app.metrics.HasBeanObject;

/**
 * This Receiver is used to request mbeans. It must be closed. Otherwise, the true connection will
 * get leaked.
 */
public interface Receiver extends AutoCloseable {

  /** @return host of jmx server */
  String host();

  /** @return port of jmx server */
  int port();

  /**
   * This method may request the latest mbeans if the current mbeans are out-of-date.
   *
   * @return current mbeans.
   */
  Collection<HasBeanObject> current();

  @Override
  void close();
}
