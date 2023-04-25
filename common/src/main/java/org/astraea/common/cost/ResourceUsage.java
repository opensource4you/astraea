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
package org.astraea.common.cost;

import java.util.HashMap;
import java.util.Map;

/** A collection of resource usage stat. */
public class ResourceUsage {

  private final Map<String, Double> usage;

  public ResourceUsage() {
    this.usage = new HashMap<>();
  }

  public ResourceUsage(Map<String, Double> usage) {
    this.usage = new HashMap<>(usage);
  }

  public Map<String, Double> usage() {
    return usage;
  }

  public void mergeUsage(ResourceUsage resourceUsage) {
    resourceUsage.usage.forEach(
        (resource, usage) ->
            this.usage.put(resource, this.usage.getOrDefault(resource, 0.0) + usage));
  }

  public void removeUsage(ResourceUsage resourceUsage) {
    resourceUsage.usage.forEach(
        (resource, usage) ->
            this.usage.put(resource, this.usage.getOrDefault(resource, 0.0) - usage));
  }

  @Override
  public String toString() {
    return "ResourceUsage{" + "usage=" + usage + '}';
  }
}
