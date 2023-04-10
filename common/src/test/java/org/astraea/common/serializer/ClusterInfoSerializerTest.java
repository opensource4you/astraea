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
package org.astraea.common.serializer;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.consumer.Deserializer;
import org.astraea.common.producer.Serializer;
import org.astraea.it.Service;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ClusterInfoSerializerTest {
  private static final Service SERVICE = Service.builder().numberOfBrokers(3).build();

  @AfterAll
  static void closeService() {
    SERVICE.close();
  }

  @Test
  void testSerializationDeserialization() {
    var topic = Utils.randomString();
    try (var admin = Admin.of(SERVICE.bootstrapServers())) {
      admin
          .creator()
          .topic(topic)
          .numberOfPartitions(1)
          .numberOfReplicas((short) 3)
          .run()
          .toCompletableFuture()
          .join();
      Utils.sleep(Duration.ofSeconds(1));
      var clusterInfo = admin.clusterInfo(Set.of(topic)).toCompletableFuture().join();
      var serializer = new Serializer.ClusterInfoSerializer();
      var deserializer = new Deserializer.ClusterInfoDeserializer();

      Assertions.assertDoesNotThrow(() -> serializer.serialize("ignore", List.of(), clusterInfo));
      var bytes = serializer.serialize("ignore", List.of(), clusterInfo);
      Assertions.assertDoesNotThrow(() -> deserializer.deserialize("ignore", List.of(), bytes));
      var deserializedClusterInfo = deserializer.deserialize("ignore", List.of(), bytes);

      Assertions.assertEquals(clusterInfo.clusterId(), deserializedClusterInfo.clusterId());
      Assertions.assertTrue(clusterInfo.nodes().containsAll(deserializedClusterInfo.nodes()));
      Assertions.assertEquals(
          clusterInfo.topics().keySet(), deserializedClusterInfo.topics().keySet());
      Assertions.assertEquals(
          clusterInfo.topics().get(topic).name(),
          deserializedClusterInfo.topics().get(topic).name());
      Assertions.assertEquals(
          clusterInfo.topics().get(topic).topicPartitions(),
          deserializedClusterInfo.topics().get(topic).topicPartitions());
      Assertions.assertEquals(
          clusterInfo.topics().get(topic).config().raw(),
          deserializedClusterInfo.topics().get(topic).config().raw());
      Assertions.assertEquals(
          clusterInfo.topics().get(topic).internal(),
          deserializedClusterInfo.topics().get(topic).internal());
      Assertions.assertTrue(deserializedClusterInfo.replicas().containsAll(clusterInfo.replicas()));
    }
  }

  @Test
  void testSerializeEmptyClusterInfo() {
    var clusterInfo = ClusterInfo.empty();
    var serializedInfo =
        Serializer.CLUSTER_INFO.serialize("topic", Collections.emptyList(), clusterInfo);
    var deserializedClusterInfo =
        Deserializer.CLUSTER_INFO.deserialize("topic", Collections.emptyList(), serializedInfo);

    Assertions.assertEquals(clusterInfo.clusterId(), deserializedClusterInfo.clusterId());
    Assertions.assertEquals(clusterInfo.nodes(), deserializedClusterInfo.nodes());
    Assertions.assertEquals(clusterInfo.topics(), deserializedClusterInfo.topics());
    Assertions.assertEquals(clusterInfo.replicas(), deserializedClusterInfo.replicas());
  }
}
