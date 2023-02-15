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

import java.util.Optional;

public interface AppInfo extends HasBeanObject {

  default String id() {
    var result = beanObject().properties().get("client-id");
    if (result == null) result = beanObject().properties().get("id");
    return result;
  }

  default String commitId() {
    var result = beanObject().attributes().get("commit-id");
    if (result == null) result = beanObject().attributes().get("CommitId");
    return (String) result;
  }

  default Optional<Long> startTimeMs() {
    var result = beanObject().attributes().get("start-time-ms");
    if (result == null) result = beanObject().attributes().get("StartTimeMs");
    if (result == null) return Optional.empty();
    return Optional.of((long) result);
  }

  default String version() {
    var result = beanObject().attributes().get("version");
    if (result == null) result = beanObject().attributes().get("Version");
    return (String) result;
  }
}
