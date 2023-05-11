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

import java.util.Map;
import java.util.Set;
import org.astraea.common.admin.TopicPartition;

public interface SinkTaskContext {

  Map<String, String> configs();

  void offset(Map<TopicPartition, Long> offsets);

  void offset(TopicPartition topicPartition, long offset);

  void timeout(long timeout);

  Set<TopicPartition> assignment();

  void pause(TopicPartition... partitions);

  void resume(TopicPartition... partitions);

  void requestCommit();

  static SinkTaskContextBuilder builder(org.apache.kafka.connect.sink.SinkTaskContext context) {
    return new SinkTaskContextBuilder(context);
  }
}
