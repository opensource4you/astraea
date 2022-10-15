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
package org.astraea.common.metrics.client.consumer;

import org.astraea.common.metrics.HasBeanObject;

public interface HasConsumerMetrics extends HasBeanObject {

  default String clientId() {
    return beanObject().properties().get("client-id");
  }

  default double reauthenticationLatencyAvg() {
    return (double) beanObject().attributes().get("reauthentication-latency-avg");
  }

  default double ioTimeNsTotal() {
    return (double) beanObject().attributes().get("io-time-ns-total");
  }

  default double successfulAuthenticationTotal() {
    return (double) beanObject().attributes().get("successful-authentication-total");
  }

  default double committedTimeNsTotal() {
    return (double) beanObject().attributes().get("committed-time-ns-total");
  }

  default double reauthenticationLatencyMax() {
    return (double) beanObject().attributes().get("reauthentication-latency-max");
  }

  default double successfulAuthenticationRate() {
    return (double) beanObject().attributes().get("successful-authentication-rate");
  }

  default double commitSyncTimeNsTotal() {
    return (double) beanObject().attributes().get("commit-sync-time-ns-total");
  }

  default double failedAuthenticationTotal() {
    return (double) beanObject().attributes().get("failed-authentication-total");
  }

  default double timeBetweenPollAvg() {
    return (double) beanObject().attributes().get("time-between-poll-avg");
  }

  default double connectionCount() {
    return (double) beanObject().attributes().get("connection-count");
  }

  default double responseTotal() {
    return (double) beanObject().attributes().get("response-total");
  }

  default double requestRate() {
    return (double) beanObject().attributes().get("request-rate");
  }

  default double incomingByteRate() {
    return (double) beanObject().attributes().get("incoming-byte-rate");
  }

  default double lastPollSecondsAgo() {
    return (double) beanObject().attributes().get("last-poll-seconds-ago");
  }

  default double successfulAuthenticationNoReauthTotal() {
    return (double) beanObject().attributes().get("successful-authentication-no-reauth-total");
  }

  default double timeBetweenPollMax() {
    return (double) beanObject().attributes().get("time-between-poll-max");
  }

  default double failedReauthenticationTotal() {
    return (double) beanObject().attributes().get("failed-reauthentication-total");
  }

  default double selectRate() {
    return (double) beanObject().attributes().get("select-rate");
  }

  default double successfulReauthenticationTotal() {
    return (double) beanObject().attributes().get("successful-reauthentication-total");
  }

  default double requestTotal() {
    return (double) beanObject().attributes().get("request-total");
  }

  default double ioTimeNsAvg() {
    return (double) beanObject().attributes().get("io-time-ns-avg");
  }

  default double ioWaitTimeNsTotal() {
    return (double) beanObject().attributes().get("io-wait-time-ns-total");
  }

  default double networkIoRate() {
    return (double) beanObject().attributes().get("network-io-rate");
  }

  default double connectionCreationRate() {
    return (double) beanObject().attributes().get("connection-creation-rate");
  }

  default double requestSizeAvg() {
    return (double) beanObject().attributes().get("request-size-avg");
  }

  default double responseRate() {
    return (double) beanObject().attributes().get("response-rate");
  }

  default double successfulReauthenticationRate() {
    return (double) beanObject().attributes().get("successful-reauthentication-rate");
  }

  default double selectTotal() {
    return (double) beanObject().attributes().get("select-total");
  }

  default double outgoingByteTotal() {
    return (double) beanObject().attributes().get("outgoing-byte-total");
  }

  default double pollIdleRatioAvg() {
    return (double) beanObject().attributes().get("poll-idle-ratio-avg");
  }

  default double connectionCloseRate() {
    return (double) beanObject().attributes().get("connection-close-rate");
  }

  default double incomingByteTotal() {
    return (double) beanObject().attributes().get("incoming-byte-total");
  }

  default double requestSizeMax() {
    return (double) beanObject().attributes().get("request-size-max");
  }

  default double failedAuthenticationRate() {
    return (double) beanObject().attributes().get("failed-authentication-rate");
  }

  default double outgoingByteRate() {
    return (double) beanObject().attributes().get("outgoing-byte-rate");
  }

  default double failedReauthenticationRate() {
    return (double) beanObject().attributes().get("failed-reauthentication-rate");
  }

  default double ioWaitTimeNsAvg() {
    return (double) beanObject().attributes().get("io-wait-time-ns-avg");
  }

  default double connectionCloseTotal() {
    return (double) beanObject().attributes().get("connection-close-total");
  }

  default double connectionCreationTotal() {
    return (double) beanObject().attributes().get("connection-creation-total");
  }

  default double networkIoTotal() {
    return (double) beanObject().attributes().get("network-io-total");
  }
}
