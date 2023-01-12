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
package org.astraea.common.admin;

public final class AdminConfigs {
  public static final String BOOTSTRAP_SERVERS_CONFIG = "bootstrap.servers";
  public static final String CLIENT_DNS_LOOKUP_CONFIG = "client.dns.lookup";
  public static final String RECONNECT_BACKOFF_MS_CONFIG = "reconnect.backoff.ms";
  public static final String RECONNECT_BACKOFF_MAX_MS_CONFIG = "reconnect.backoff.max.ms";
  public static final String RETRY_BACKOFF_MS_CONFIG = "retry.backoff.ms";
  public static final String SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG =
      "socket.connection.setup.timeout.ms";
  public static final String SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG =
      "socket.connection.setup.timeout.max.ms";
  public static final String CONNECTIONS_MAX_IDLE_MS_CONFIG = "connections.max.idle.ms";
  public static final String REQUEST_TIMEOUT_MS_CONFIG = "request.timeout.ms";

  public static final String CLIENT_ID_CONFIG = "client.id";

  public static final String METADATA_MAX_AGE_CONFIG = "metadata.max.age.ms";

  public static final String SEND_BUFFER_CONFIG = "send.buffer.bytes";

  public static final String RECEIVE_BUFFER_CONFIG = "receive.buffer.bytes";

  public static final String METRIC_REPORTER_CLASSES_CONFIG = "metric.reporters";

  public static final String METRICS_NUM_SAMPLES_CONFIG = "metrics.num.samples";

  public static final String METRICS_SAMPLE_WINDOW_MS_CONFIG = "metrics.sample.window.ms";

  public static final String METRICS_RECORDING_LEVEL_CONFIG = "metrics.recording.level";

  public static final String RETRIES_CONFIG = "retries";

  private AdminConfigs() {}
}
