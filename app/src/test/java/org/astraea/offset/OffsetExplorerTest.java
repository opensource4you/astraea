package org.astraea.offset;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class OffsetExplorerTest {

  @Test
  void testToAdminProps() {
    Assertions.assertEquals(
        "brokers",
        OffsetExplorer.toAdminProps("brokers").get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG));
  }

  @Test
  void testExecute() {
    var topicName = "topic";
    var partition = 1000;
    var latestOffset = 1000L;
    var earliestOffset = 100L;

    try (var admin =
        new OffsetExplorer.Admin() {
          @Override
          public void close() {}

          @Override
          public Set<String> topics() {
            return Collections.singleton("topic");
          }

          @Override
          public Set<TopicPartition> partitions(Set<String> topics) {
            return Collections.singleton(new TopicPartition(topicName, partition));
          }

          @Override
          public Map<TopicPartition, Long> earliestOffsets(Set<TopicPartition> partitions) {
            return Map.of(new TopicPartition(topicName, partition), earliestOffset);
          }

          @Override
          public Map<TopicPartition, Long> latestOffsets(Set<TopicPartition> partitions) {
            return Map.of(new TopicPartition(topicName, partition), latestOffset);
          }
        }) {
      var result = OffsetExplorer.execute(admin, Collections.singleton(topicName));
      Assertions.assertEquals(1, result.size());
      var item = result.entrySet().iterator().next();
      Assertions.assertEquals(topicName, item.getKey().topic());
      Assertions.assertEquals(partition, item.getKey().partition());
      Assertions.assertEquals(earliestOffset, item.getValue().getKey());
      Assertions.assertEquals(latestOffset, item.getValue().getValue());
    }
  }
}
