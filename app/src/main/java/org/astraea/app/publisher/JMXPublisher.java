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
package org.astraea.app.publisher;

import java.util.concurrent.CompletionStage;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.producer.Producer;
import org.astraea.common.producer.Record;
import org.astraea.common.producer.Serializer;

public interface JMXPublisher extends AutoCloseable {
  static JMXPublisher create(String bootstrap) {
    var producer =
        Producer.builder().bootstrapServers(bootstrap).valueSerializer(Serializer.STRING).build();
    return new JMXPublisher() {
      @Override
      public CompletionStage<Void> publish(String id, BeanObject bean) {
        Record<byte[], String> record =
            Record.builder()
                .topic(MetricPublisher.internalTopicName(id))
                .key((byte[]) null)
                .value(bean.toString())
                .build();
        return producer.send(record).thenAccept(ignore -> {});
      }

      @Override
      public void close() {
        producer.close();
      }
    };
  }

  CompletionStage<Void> publish(String id, BeanObject bean);

  void close();
}
