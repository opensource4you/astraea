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
package org.astraea.common.connector;

import java.util.Map;
import java.util.Optional;

/** this is not a kind of json response from kafka. */
public class TaskStatus {

  private final String connectorName;
  private final int id;
  private final String state;

  private final String workerId;

  private final Map<String, String> configs;

  private final Optional<String> error;

  TaskStatus(
      String connectorName,
      int id,
      String state,
      String workerId,
      Map<String, String> configs,
      Optional<String> error) {
    this.connectorName = connectorName;
    this.id = id;
    this.state = state;
    this.workerId = workerId;
    this.configs = Map.copyOf(configs);
    this.error = error;
  }

  public String connectorName() {
    return connectorName;
  }

  public int id() {
    return id;
  }

  public String state() {
    return state;
  }

  public String workerId() {
    return workerId;
  }

  public Map<String, String> configs() {
    return configs;
  }

  public Optional<String> error() {
    return error;
  }
}
