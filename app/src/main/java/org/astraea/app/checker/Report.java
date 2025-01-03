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
package org.astraea.app.checker;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.common.Node;

public record Report(Node node, String why) {
  static Report noMetrics(Node node) {
    return new Report(node, "failed to get metrics from");
  }

  static Report of(Node node, String why) {
    return new Report(node, why);
  }

  static Report empty() {
    return new Report(null, "");
  }

  static Report of(Node node, Protocol protocol, Set<Integer> versions) {
    var unsupportedVersions =
        versions.stream().filter(v -> v < protocol.base()).collect(Collectors.toSet());
    if (unsupportedVersions.isEmpty()) return empty();
    return new Report(
        node,
        String.format(
            "there are unsupported %s versions: %s due to new baseline: %s",
            protocol.name(), unsupportedVersions, protocol.base()));
  }

  Stream<Report> stream() {
    if (why.isEmpty()) return Stream.empty();
    return Stream.of(this);
  }

  @Override
  public String toString() {
    if (node == null) {
      return "Report[pass]";
    }
    return "Report[" + node + "]  why = " + why;
  }
}
