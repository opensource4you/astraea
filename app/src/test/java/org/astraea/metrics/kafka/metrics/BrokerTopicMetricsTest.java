package org.astraea.metrics.kafka.metrics;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class BrokerTopicMetricsTest {

  @Test
  void testAllEnumNameUnique() {
    // arrange act
    Set<String> collectedName =
        Arrays.stream(BrokerTopicMetrics.values())
            .map(BrokerTopicMetrics::metricName)
            .collect(Collectors.toSet());

    // assert
    assertEquals(BrokerTopicMetrics.values().length, collectedName.size());
  }
}
