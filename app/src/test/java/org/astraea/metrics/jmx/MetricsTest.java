package org.astraea.metrics.jmx;

import static org.astraea.metrics.jmx.Metrics.*;

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
      Assertions.assertDoesNotThrow(() -> BrokerTopic.MessageInPerSecond.count);
      Assertions.assertDoesNotThrow(() -> BrokerTopic.MessageInPerSecond.fifteenMinuteRate);
      Assertions.assertDoesNotThrow(() -> BrokerTopic.MessageInPerSecond.fiveMinuteRate);
      Assertions.assertDoesNotThrow(() -> BrokerTopic.MessageInPerSecond.meanRate);
      Assertions.assertDoesNotThrow(() -> BrokerTopic.MessageInPerSecond.oneMinuteRate);

      Assertions.assertDoesNotThrow(() -> BrokerTopic.BytesInPerSecond.count);
      Assertions.assertDoesNotThrow(() -> BrokerTopic.BytesInPerSecond.fifteenMinuteRate);
      Assertions.assertDoesNotThrow(() -> BrokerTopic.BytesInPerSecond.fiveMinuteRate);
      Assertions.assertDoesNotThrow(() -> BrokerTopic.BytesInPerSecond.meanRate);
      Assertions.assertDoesNotThrow(() -> BrokerTopic.BytesInPerSecond.oneMinuteRate);

      Assertions.assertDoesNotThrow(() -> BrokerTopic.BytesOutPerSecond.count);
      Assertions.assertDoesNotThrow(() -> BrokerTopic.BytesOutPerSecond.fifteenMinuteRate);
      Assertions.assertDoesNotThrow(() -> BrokerTopic.BytesOutPerSecond.fiveMinuteRate);
      Assertions.assertDoesNotThrow(() -> BrokerTopic.BytesOutPerSecond.meanRate);
      Assertions.assertDoesNotThrow(() -> BrokerTopic.BytesOutPerSecond.oneMinuteRate);

      Assertions.assertDoesNotThrow(() -> BrokerTopic.ReplicationBytesInPerSec.count);
      Assertions.assertDoesNotThrow(() -> BrokerTopic.ReplicationBytesInPerSec.fifteenMinuteRate);
      Assertions.assertDoesNotThrow(() -> BrokerTopic.ReplicationBytesInPerSec.fiveMinuteRate);
      Assertions.assertDoesNotThrow(() -> BrokerTopic.ReplicationBytesInPerSec.meanRate);
      Assertions.assertDoesNotThrow(() -> BrokerTopic.ReplicationBytesInPerSec.oneMinuteRate);

      Assertions.assertDoesNotThrow(() -> BrokerTopic.ReplicationBytesOutPerSec.count);
      Assertions.assertDoesNotThrow(() -> BrokerTopic.ReplicationBytesOutPerSec.fifteenMinuteRate);
      Assertions.assertDoesNotThrow(() -> BrokerTopic.ReplicationBytesOutPerSec.fiveMinuteRate);
      Assertions.assertDoesNotThrow(() -> BrokerTopic.ReplicationBytesOutPerSec.meanRate);
      Assertions.assertDoesNotThrow(() -> BrokerTopic.ReplicationBytesOutPerSec.oneMinuteRate);
    }

    @Test
    void jvmMemory() {
      Assertions.assertDoesNotThrow(() -> JvmMemory.heapMemoryUsage);
      Assertions.assertDoesNotThrow(() -> JvmMemory.nonHeapMemoryUsage);
    }

    @Test
    void socketServer() {
      Assertions.assertDoesNotThrow(() -> SocketServer.networkProcessorAvgIdlePercent);
    }
  }
}
