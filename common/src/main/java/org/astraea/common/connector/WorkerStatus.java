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

/** this is not a kind of json response from kafka. We compose it with worker hostname and port. */
public class WorkerStatus {

  private final String hostname;

  private final int port;

  private final String version;
  private final String commit;

  private final String kafkaClusterId;

  private final long numberOfConnectors;

  private final long numberOfTasks;

  WorkerStatus(
      String hostname,
      int port,
      String version,
      String commit,
      String kafkaClusterId,
      long numberOfConnectors,
      long numberOfTasks) {
    this.hostname = hostname;
    this.port = port;
    this.version = version;
    this.commit = commit;
    this.kafkaClusterId = kafkaClusterId;
    this.numberOfConnectors = numberOfConnectors;
    this.numberOfTasks = numberOfTasks;
  }

  public String hostname() {
    return hostname;
  }

  public int port() {
    return port;
  }

  public String version() {
    return version;
  }

  public String commit() {
    return commit;
  }

  public String kafkaClusterId() {
    return kafkaClusterId;
  }

  public long numberOfConnectors() {
    return numberOfConnectors;
  }

  public long numberOfTasks() {
    return numberOfTasks;
  }
}
