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
package org.astraea.app.metrics.producer;

import org.astraea.app.metrics.HasBeanObject;

public interface HasProducerNodeMetrics extends HasBeanObject {

  default double incomingByteRate() {
    return (double) beanObject().getAttributes().get("incoming-byte-rate");
  }

  default double incomingByteTotal() {
    return (double) beanObject().getAttributes().get("incoming-byte-total");
  }

  default double outgoingByteRate() {
    return (double) beanObject().getAttributes().get("outgoing-byte-rate");
  }

  default double outgoingByteTotal() {
    return (double) beanObject().getAttributes().get("outgoing-byte-total");
  }

  default double requestLatencyAvg() {
    return (double) beanObject().getAttributes().get("request-latency-avg");
  }

  default double requestLatencyMax() {
    return (double) beanObject().getAttributes().get("request-latency-max");
  }

  default double requestRate() {
    return (double) beanObject().getAttributes().get("request-rate");
  }

  default double requestSizeAvg() {
    return (double) beanObject().getAttributes().get("request-size-avg");
  }

  default double requestSizeMax() {
    return (double) beanObject().getAttributes().get("request-size-max");
  }

  default double requestTotal() {
    return (double) beanObject().getAttributes().get("request-total");
  }

  default double responseRate() {
    return (double) beanObject().getAttributes().get("response-rate");
  }

  default double responseTotal() {
    return (double) beanObject().getAttributes().get("response-total");
  }
}
