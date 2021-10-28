package org.astraea.moveCost;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;
import org.astraea.service.RequireBrokerCluster;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class CalculateUtilsTest extends RequireBrokerCluster {
  static Map<Integer, Map<TopicPartition, Integer>> fakeBroker_PartitionSize = new HashMap<>();
  static Map<String, Integer> fakeRetention_ms = new HashMap<>();
  static Map<Integer, Map<TopicPartition, Double>> fakePartitionLoad = new HashMap<>();

  @BeforeAll
  static void setup() {
    // set partition size and retention time
    Map<TopicPartition, Integer> fakePartitionSize = new HashMap<>();
    fakePartitionSize.put(new TopicPartition("test0", 0), 100000000);
    fakePartitionSize.put(new TopicPartition("test0", 1), 200000000);
    fakePartitionSize.put(new TopicPartition("test0", 2), 300000000);
    fakePartitionSize.put(new TopicPartition("test0", 3), 400000000);
    fakeBroker_PartitionSize.put(0, fakePartitionSize);
    fakePartitionSize = new HashMap<>();
    fakePartitionSize.put(new TopicPartition("test1", 0), 500000000);
    fakePartitionSize.put(new TopicPartition("test1", 1), 600000000);
    fakePartitionSize.put(new TopicPartition("test1", 2), 700000000);
    fakePartitionSize.put(new TopicPartition("test1", 3), 800000000);
    fakeBroker_PartitionSize.put(1, fakePartitionSize);
    fakeRetention_ms.put("test0", 604800000);
    fakeRetention_ms.put("test1", 604800000);

    // set partition load
    fakePartitionLoad = new HashMap<>();
    Map<TopicPartition, Double> fakeTopicLoad = new HashMap<>();
    fakeTopicLoad.put(new TopicPartition("test0", 0), 0.1);
    fakeTopicLoad.put(new TopicPartition("test0", 1), 0.2);
    fakeTopicLoad.put(new TopicPartition("test0", 2), 0.3);
    fakeTopicLoad.put(new TopicPartition("test0", 3), 0.4);
    fakePartitionLoad.put(0, fakeTopicLoad);
    fakeTopicLoad = new HashMap<>();
    fakeTopicLoad.put(new TopicPartition("test1", 0), 1.5);
    fakeTopicLoad.put(new TopicPartition("test1", 1), 1.6);
    fakeTopicLoad.put(new TopicPartition("test1", 2), 1.7);
    fakeTopicLoad.put(new TopicPartition("test1", 3), 1.8);
    fakePartitionLoad.put(1, fakeTopicLoad);
  }

  @Test
  void testGetLoad() {
    var Load = CalculateUtils.getLoad(fakeBroker_PartitionSize, fakeRetention_ms);
    assertEquals(2, Load.size());
    assertEquals(4, Load.get(0).size());
    assertEquals(4, Load.get(1).size());
    assertEquals(0.165, round(Load.get(0).get(new TopicPartition("test0", 0))));
    assertEquals(0.331, round(Load.get(0).get(new TopicPartition("test0", 1))));
    assertEquals(0.496, round(Load.get(0).get(new TopicPartition("test0", 2))));
    assertEquals(0.661, round(Load.get(0).get(new TopicPartition("test0", 3))));
    assertEquals(0.827, round(Load.get(1).get(new TopicPartition("test1", 0))));
    assertEquals(0.992, round(Load.get(1).get(new TopicPartition("test1", 1))));
    assertEquals(1.157, round(Load.get(1).get(new TopicPartition("test1", 2))));
    assertEquals(1.323, round(Load.get(1).get(new TopicPartition("test1", 3))));
  }

  @Test
  void testGetScore() {
    var Score = CalculateUtils.getScore(fakePartitionLoad);
    assertEquals(2, Score.size());
    assertEquals(4, Score.get(0).size());
    assertEquals(4, Score.get(1).size());
    assertEquals(0.0, round(Score.get(0).get(new TopicPartition("test0", 0))));
    assertEquals(0.0, round(Score.get(0).get(new TopicPartition("test0", 1))));
    assertEquals(0.0, round(Score.get(0).get(new TopicPartition("test0", 2))));
    assertEquals(0.0, round(Score.get(0).get(new TopicPartition("test0", 3))));
    assertEquals(-80.498, round(Score.get(1).get(new TopicPartition("test1", 0))));
    assertEquals(-26.833, round(Score.get(1).get(new TopicPartition("test1", 1))));
    assertEquals(26.833, round(Score.get(1).get(new TopicPartition("test1", 2))));
    assertEquals(80.498, round(Score.get(1).get(new TopicPartition("test1", 3))));
  }

  double round(double score) {
    return Math.round(1000 * score) / 1000.0;
  }
}
