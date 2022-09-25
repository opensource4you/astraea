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

public class WorkerInfo {
  private final String version;
  private final String commit;
  private final String kafkaClusterId;

  public WorkerInfo(String version, String commit, String kafkaClusterId) {
    this.version = version;
    this.commit = commit;
    this.kafkaClusterId = kafkaClusterId;
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

  @Override
  public String toString() {
    return "WorkerInfo{"
        + "version='"
        + version
        + '\''
        + ", commit='"
        + commit
        + '\''
        + ", kafkaClusterId='"
        + kafkaClusterId
        + '\''
        + '}';
  }
}
