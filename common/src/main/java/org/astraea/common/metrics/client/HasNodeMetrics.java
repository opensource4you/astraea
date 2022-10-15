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
package org.astraea.common.metrics.client;

import java.util.function.Function;
import org.astraea.common.metrics.HasBeanObject;

public interface HasNodeMetrics extends HasBeanObject {
  Function<String, Integer> BROKER_ID_FETCHER =
      node -> Integer.parseInt(node.substring(node.indexOf("-") + 1));

  default int brokerId() {
    return BROKER_ID_FETCHER.apply(beanObject().properties().get("node-id"));
  }

  default double incomingByteRate() {
    return (double) beanObject().attributes().get("incoming-byte-rate");
  }

  default double incomingByteTotal() {
    return (double) beanObject().attributes().get("incoming-byte-total");
  }

  default double outgoingByteRate() {
    return (double) beanObject().attributes().get("outgoing-byte-rate");
  }

  default double outgoingByteTotal() {
    return (double) beanObject().attributes().get("outgoing-byte-total");
  }

  default double requestLatencyAvg() {
    return (double) beanObject().attributes().get("request-latency-avg");
  }

  default double requestLatencyMax() {
    return (double) beanObject().attributes().get("request-latency-max");
  }

  default double requestRate() {
    return (double) beanObject().attributes().get("request-rate");
  }

  default double requestSizeAvg() {
    return (double) beanObject().attributes().get("request-size-avg");
  }

  default double requestSizeMax() {
    return (double) beanObject().attributes().get("request-size-max");
  }

  default double requestTotal() {
    return (double) beanObject().attributes().get("request-total");
  }

  default double responseRate() {
    return (double) beanObject().attributes().get("response-rate");
  }

  default double responseTotal() {
    return (double) beanObject().attributes().get("response-total");
  }
}
