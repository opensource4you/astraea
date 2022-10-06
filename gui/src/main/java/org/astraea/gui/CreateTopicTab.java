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
package org.astraea.gui;

import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import javafx.scene.control.Tab;
import org.astraea.common.LinkedHashSet;
import org.astraea.common.admin.TopicCreator;

public class CreateTopicTab {

  private static final LinkedHashSet<String> ALL_CONFIG_KEYS =
      LinkedHashSet.of(
          TopicCreator.SEGMENT_BYTES_CONFIG,
          TopicCreator.SEGMENT_MS_CONFIG,
          TopicCreator.SEGMENT_JITTER_MS_CONFIG,
          TopicCreator.SEGMENT_INDEX_BYTES_CONFIG,
          TopicCreator.FLUSH_MESSAGES_INTERVAL_CONFIG,
          TopicCreator.FLUSH_MS_CONFIG,
          TopicCreator.RETENTION_BYTES_CONFIG,
          TopicCreator.RETENTION_MS_CONFIG,
          TopicCreator.REMOTE_LOG_STORAGE_ENABLE_CONFIG,
          TopicCreator.LOCAL_LOG_RETENTION_MS_CONFIG,
          TopicCreator.LOCAL_LOG_RETENTION_BYTES_CONFIG,
          TopicCreator.MAX_MESSAGE_BYTES_CONFIG,
          TopicCreator.INDEX_INTERVAL_BYTES_CONFIG,
          TopicCreator.FILE_DELETE_DELAY_MS_CONFIG,
          TopicCreator.DELETE_RETENTION_MS_CONFIG,
          TopicCreator.MIN_COMPACTION_LAG_MS_CONFIG,
          TopicCreator.MAX_COMPACTION_LAG_MS_CONFIG,
          TopicCreator.MIN_CLEANABLE_DIRTY_RATIO_CONFIG,
          TopicCreator.CLEANUP_POLICY_CONFIG,
          TopicCreator.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG,
          TopicCreator.MIN_IN_SYNC_REPLICAS_CONFIG,
          TopicCreator.COMPRESSION_TYPE_CONFIG,
          TopicCreator.PREALLOCATE_CONFIG,
          TopicCreator.MESSAGE_TIMESTAMP_TYPE_CONFIG,
          TopicCreator.MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_CONFIG,
          TopicCreator.MESSAGE_DOWNCONVERSION_ENABLE_CONFIG);

  public static Tab of(Context context) {
    var tab = new Tab("create topic");
    tab.setContent(
        Utils.form(
            LinkedHashSet.of("topic", "partitions", "replicas"),
            ALL_CONFIG_KEYS,
            (result, console) -> {
              var allConfigs = new HashMap<>(result);
              var name = allConfigs.remove("name");
              if (name == null)
                return CompletableFuture.failedFuture(
                    new IllegalArgumentException("please define topic name"));
              return context.submit(
                  admin ->
                      admin
                          .topicNames(true)
                          .thenCompose(
                              names -> {
                                if (names.contains(name))
                                  return CompletableFuture.completedFuture(
                                      name + " is already existent");

                                return admin
                                    .creator()
                                    .topic(name)
                                    .numberOfPartitions(
                                        Optional.ofNullable(allConfigs.remove("partitions"))
                                            .map(Integer::parseInt)
                                            .orElse(1))
                                    .numberOfReplicas(
                                        Optional.ofNullable(allConfigs.remove("replicas"))
                                            .map(Short::parseShort)
                                            .orElse((short) 1))
                                    .configs(allConfigs)
                                    .run()
                                    .thenApply(i -> "succeed to create topic:" + name);
                              }));
            },
            "CREATE"));

    return tab;
  }
}
