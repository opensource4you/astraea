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

      Assertions.assertDoesNotThrow(() -> BrokerTopic.TotalFetchRequestsPerSec.count);
      Assertions.assertDoesNotThrow(() -> BrokerTopic.TotalFetchRequestsPerSec.fifteenMinuteRate);
      Assertions.assertDoesNotThrow(() -> BrokerTopic.TotalFetchRequestsPerSec.fiveMinuteRate);
      Assertions.assertDoesNotThrow(() -> BrokerTopic.TotalFetchRequestsPerSec.meanRate);
      Assertions.assertDoesNotThrow(() -> BrokerTopic.TotalFetchRequestsPerSec.oneMinuteRate);

      Assertions.assertDoesNotThrow(() -> BrokerTopic.TotalProduceRequestsPerSec.count);
      Assertions.assertDoesNotThrow(() -> BrokerTopic.TotalProduceRequestsPerSec.fifteenMinuteRate);
      Assertions.assertDoesNotThrow(() -> BrokerTopic.TotalProduceRequestsPerSec.fiveMinuteRate);
      Assertions.assertDoesNotThrow(() -> BrokerTopic.TotalProduceRequestsPerSec.meanRate);
      Assertions.assertDoesNotThrow(() -> BrokerTopic.TotalProduceRequestsPerSec.oneMinuteRate);
    }

    @Test
    void requestMetrics() {
      RequestLatencyMetric testingRequest = RequestLatencyMetric.of("Testing");
      RequestLatencyMetric.MeasuredValue[] measuredValues =
          new RequestLatencyMetric.MeasuredValue[] {
            testingRequest.localTimeMs,
            testingRequest.requestBytes,
            testingRequest.remoteTimeMs,
            testingRequest.requestQueueTimeMs,
            testingRequest.responseQueueTimeMs,
            testingRequest.responseSendTimeMs,
            testingRequest.throttleTimeMs,
            testingRequest.totalTimeMs
          };
      for (RequestLatencyMetric.MeasuredValue measuredValue : measuredValues) {
        Assertions.assertDoesNotThrow(measuredValue::mean);
        Assertions.assertDoesNotThrow(measuredValue::max);
        Assertions.assertDoesNotThrow(measuredValue::min);
        Assertions.assertDoesNotThrow(measuredValue::stddev);
        Assertions.assertDoesNotThrow(measuredValue::percentile50);
        Assertions.assertDoesNotThrow(measuredValue::percentile75);
        Assertions.assertDoesNotThrow(measuredValue::percentile95);
        Assertions.assertDoesNotThrow(measuredValue::percentile98);
        Assertions.assertDoesNotThrow(measuredValue::percentile99);
        Assertions.assertDoesNotThrow(measuredValue::percentile999);
      }
    }

    @Test
    void processor() {
      Assertions.assertDoesNotThrow(() -> ProcessorThread.of(0).idlePercent);
      Assertions.assertDoesNotThrow(() -> ProcessorThread.of(1).idlePercent);
      Assertions.assertDoesNotThrow(() -> ProcessorThread.of(2).idlePercent);
      Assertions.assertDoesNotThrow(() -> ProcessorThread.of(3).idlePercent);
      Assertions.assertDoesNotThrow(() -> ProcessorThread.of(4).idlePercent);
      Assertions.assertDoesNotThrow(() -> ProcessorThread.of(5).idlePercent);
    }

    @Test
    void jvmMemory() {
      Assertions.assertDoesNotThrow(() -> JvmMemory.heapMemoryUsage);
      Assertions.assertDoesNotThrow(() -> JvmMemory.nonHeapMemoryUsage);
    }

    @Test
    void jvmOperatingSystem() {
      Assertions.assertDoesNotThrow(() -> JvmOperatingSystem.arch);
      Assertions.assertDoesNotThrow(() -> JvmOperatingSystem.name);
      Assertions.assertDoesNotThrow(() -> JvmOperatingSystem.version);
      Assertions.assertDoesNotThrow(() -> JvmOperatingSystem.availableProcessors);
      Assertions.assertDoesNotThrow(() -> JvmOperatingSystem.committedVirtualMemorySize);
      Assertions.assertDoesNotThrow(() -> JvmOperatingSystem.freePhysicalMemorySize);
      Assertions.assertDoesNotThrow(() -> JvmOperatingSystem.freeSwapSpaceSize);
      Assertions.assertDoesNotThrow(() -> JvmOperatingSystem.maxFileDescriptorCount);
      Assertions.assertDoesNotThrow(() -> JvmOperatingSystem.openFileDescriptorCount);
      Assertions.assertDoesNotThrow(() -> JvmOperatingSystem.ProcessCpuLoad);
      Assertions.assertDoesNotThrow(() -> JvmOperatingSystem.ProcessCpuTime);
      Assertions.assertDoesNotThrow(() -> JvmOperatingSystem.systemCpuLoad);
      Assertions.assertDoesNotThrow(() -> JvmOperatingSystem.systemLoadAverage);
      Assertions.assertDoesNotThrow(() -> JvmOperatingSystem.totalPhysicalMemorySize);
      Assertions.assertDoesNotThrow(() -> JvmOperatingSystem.totalSwapSpaceSize);
    }

    @Test
    void socketServer() {
      Assertions.assertDoesNotThrow(() -> SocketServer.networkProcessorAvgIdlePercent);
    }

    @Test
    void disk() {
      Assertions.assertDoesNotThrow(() -> Disk.linuxDiskReadBytes);
      Assertions.assertDoesNotThrow(() -> Disk.linuxDiskWriteBytes);
    }

    @Test
    void requestHandler() {
      Assertions.assertDoesNotThrow(() -> RequestHandler.RequestHandlerAvgIdlePercent.count);
      Assertions.assertDoesNotThrow(
          () -> RequestHandler.RequestHandlerAvgIdlePercent.fifteenMinuteRate);
      Assertions.assertDoesNotThrow(
          () -> RequestHandler.RequestHandlerAvgIdlePercent.fiveMinuteRate);
      Assertions.assertDoesNotThrow(() -> RequestHandler.RequestHandlerAvgIdlePercent.meanRate);
      Assertions.assertDoesNotThrow(
          () -> RequestHandler.RequestHandlerAvgIdlePercent.oneMinuteRate);
    }
  }
}
