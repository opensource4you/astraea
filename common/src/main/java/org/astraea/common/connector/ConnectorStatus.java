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

import java.util.List;
import java.util.Map;

/** this is not a kind of json response from kafka. */
public class ConnectorStatus {

  private final String name;

  private final String state;

  private final String workerId;

  private final Map<String, String> configs;

  private final List<TaskStatus> tasks;

  ConnectorStatus(
      String name,
      String state,
      String workerId,
      Map<String, String> configs,
      List<TaskStatus> tasks) {
    this.name = name;
    this.state = state;
    this.workerId = workerId;
    this.configs = configs;
    this.tasks = tasks;
  }

  public String name() {
    return name;
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

  public List<TaskStatus> tasks() {
    return tasks;
  }
}
