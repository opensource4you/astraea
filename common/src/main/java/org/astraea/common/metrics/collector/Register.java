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

public interface Register {

  /**
   * @param host of jmx server
   * @return this register
   */
  Register host(String host);

  /**
   * @param port of jmx server
   * @return this register
   */
  Register port(int port);

  /**
   * declare a receiver based on local metrics client
   *
   * @return this register
   */
  Register local();

  /**
   * @param fetcher to get metrics from MBeanClient
   * @return this register
   */
  Register fetcher(Fetcher fetcher);

  /** @return create a Receiver */
  Receiver build();
}
