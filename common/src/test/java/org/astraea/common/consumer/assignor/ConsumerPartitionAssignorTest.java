package org.astraea.common.consumer.assignor;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ConsumerPartitionAssignorTest {
    @Test
    void testSubscriptionConvert() {
        var data = "rack=1";
        var userData = ByteBuffer.wrap(data.getBytes(StandardCharsets.UTF_8));
        var kafkaSubscription =
                new ConsumerPartitionAssignor.Subscription(List.of("test"), userData, null);
        var ourSubscription =
                org.astraea.common.consumer.assignor.Subscription.from(kafkaSubscription);

        Assertions.assertEquals(kafkaSubscription.topics(), ourSubscription.topics());
        Assertions.assertEquals(kafkaSubscription.ownedPartitions(), ourSubscription.ownedPartitions());
        Assertions.assertEquals(kafkaSubscription.groupInstanceId(), ourSubscription.groupInstanceId());
        Assertions.assertEquals("1", ourSubscription.userData().get("rack"));
        Assertions.assertNull(ourSubscription.userData().get("rack=1"));
    }

    @Test
    void testGroupSubscriptionConvert() {
        var kafkaUser1Subscription =
                new ConsumerPartitionAssignor.Subscription(
                        List.of("test1", "test2"), convert("rack=1"), null);
        var kafkaUser2Subscription =
                new ConsumerPartitionAssignor.Subscription(
                        List.of("test1", "test2"),
                        convert("rack=2"),
                        List.of(new TopicPartition("test1", 0), new TopicPartition("test2", 1)));
        kafkaUser2Subscription.setGroupInstanceId(Optional.of("astraea"));
        var kafkaGroupSubscription =
                new ConsumerPartitionAssignor.GroupSubscription(
                        Map.of("user1", kafkaUser1Subscription, "user2", kafkaUser2Subscription));
        var ourGroupSubscription =
                org.astraea.common.consumer.assignor.GroupSubscription.from(
                        kafkaGroupSubscription);

        var ourUser1Subscription = ourGroupSubscription.groupSubscription().get("user1");
        var ourUser2Subscription = ourGroupSubscription.groupSubscription().get("user2");

        Assertions.assertEquals(Optional.empty(), ourUser1Subscription.groupInstanceId());
        Assertions.assertEquals(null, ourUser1Subscription.ownedPartitions());
        Assertions.assertEquals("1", ourUser1Subscription.userData().get("rack"));
        Assertions.assertEquals(List.of("test1", "test2"), ourUser1Subscription.topics());
        Assertions.assertEquals(
                "astraea",
                ourUser2Subscription.groupInstanceId().isPresent()
                        ? ourUser2Subscription.groupInstanceId().get()
                        : Optional.empty());
        Assertions.assertEquals(
                List.of(
                        org.astraea.common.admin.TopicPartition.of("test1", 0),
                        org.astraea.common.admin.TopicPartition.of("test2", 1)),
                ourUser2Subscription.ownedPartitions());
        Assertions.assertEquals("2", ourUser2Subscription.userData().get("rack"));
        Assertions.assertEquals(List.of("test1", "test2"), ourUser2Subscription.topics());
    }

    private static ByteBuffer convert(String value) {
        return ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8));}
}
