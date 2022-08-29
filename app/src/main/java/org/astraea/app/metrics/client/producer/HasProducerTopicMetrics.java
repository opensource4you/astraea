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
package org.astraea.app.metrics.client.producer;

import org.astraea.app.metrics.HasBeanObject;

public interface HasProducerTopicMetrics extends HasBeanObject {

  default double byteRate() {
    return (double) beanObject().attributes().get("byte-rate");
  }

  default double byteTotal() {
    return (double) beanObject().attributes().get("byte-total");
  }

  default double compressionRate() {
    return (double) beanObject().attributes().get("compression-rate");
  }

  default double recordErrorRate() {
    return (double) beanObject().attributes().get("record-error-rate");
  }

  default double recordErrorTotal() {
    return (double) beanObject().attributes().get("record-error-total");
  }

  default double recordRetryRate() {
    return (double) beanObject().attributes().get("record-retry-rate");
  }

  default double recordRetryTotal() {
    return (double) beanObject().attributes().get("record-retry-total");
  }

  default double recordSendRate() {
    return (double) beanObject().attributes().get("record-send-rate");
  }

  default double recordSendTotal() {
    return (double) beanObject().attributes().get("record-send-total");
  }
}
