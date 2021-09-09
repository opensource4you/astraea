package org.astraea.metrics.jmx;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class MetricsTest {

  @Nested
  @DisplayName("Ensure all predefined metrics won't throw error during initialization")
  class EnsureAllPredefinedMetricsWillNotThrowErrorDuringInitialization {

    @Test
    void brokerTopic() {
      Assertions.assertDoesNotThrow(() -> Metrics.BrokerTopic.MessageInPerSecond.count);
      Assertions.assertDoesNotThrow(() -> Metrics.BrokerTopic.MessageInPerSecond.fifteenMinuteRate);
      Assertions.assertDoesNotThrow(() -> Metrics.BrokerTopic.MessageInPerSecond.fiveMinuteRate);
      Assertions.assertDoesNotThrow(() -> Metrics.BrokerTopic.MessageInPerSecond.meanRate);
      Assertions.assertDoesNotThrow(() -> Metrics.BrokerTopic.MessageInPerSecond.oneMinuteRate);
    }

    @Test
    void jvmMemory() {
      Assertions.assertDoesNotThrow(() -> Metrics.JvmMemory.heapMemoryUsage);
      Assertions.assertDoesNotThrow(() -> Metrics.JvmMemory.nonHeapMemoryUsage);
    }
  }
}
