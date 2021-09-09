package org.astraea.metrics.jmx;

import java.lang.management.MemoryUsage;
import org.astraea.metrics.jmx.template.StatisticsMetricsTemplate;

/** A list of all kafka metrics */
public final class Metrics {

  // This class intent to use as a collection of kafka metric object.
  // The following private construction, making this class impossible to initiate.
  private Metrics() {}

  public static class BrokerTopic {
    public static class MessageInPerSecond {

      private static final String objectName =
          "kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec";

      public static final LongBrokerMetric count = new LongBrokerMetric(objectName, "Count");
      public static final DoubleBrokerMetric fifteenMinuteRate =
          new DoubleBrokerMetric(objectName, "FifteenMinuteRate");
      public static final DoubleBrokerMetric fiveMinuteRate =
          new DoubleBrokerMetric(objectName, "FiveMinuteRate");
      public static final DoubleBrokerMetric meanRate =
          new DoubleBrokerMetric(objectName, "MeanRate");
      public static final DoubleBrokerMetric oneMinuteRate =
          new DoubleBrokerMetric(objectName, "OneMinuteRate");
    }

    public static class BytesInPerSecond {

      private static final String objectName =
          "kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec";

      public static final LongBrokerMetric count = new LongBrokerMetric(objectName, "Count");
      public static final DoubleBrokerMetric fifteenMinuteRate =
          new DoubleBrokerMetric(objectName, "FifteenMinuteRate");
      public static final DoubleBrokerMetric fiveMinuteRate =
          new DoubleBrokerMetric(objectName, "FiveMinuteRate");
      public static final DoubleBrokerMetric meanRate =
          new DoubleBrokerMetric(objectName, "MeanRate");
      public static final DoubleBrokerMetric oneMinuteRate =
          new DoubleBrokerMetric(objectName, "OneMinuteRate");
    }

    public static class BytesOutPerSecond {

      private static final String objectName =
          "kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec";

      public static final LongBrokerMetric count = new LongBrokerMetric(objectName, "Count");
      public static final DoubleBrokerMetric fifteenMinuteRate =
          new DoubleBrokerMetric(objectName, "FifteenMinuteRate");
      public static final DoubleBrokerMetric fiveMinuteRate =
          new DoubleBrokerMetric(objectName, "FiveMinuteRate");
      public static final DoubleBrokerMetric meanRate =
          new DoubleBrokerMetric(objectName, "MeanRate");
      public static final DoubleBrokerMetric oneMinuteRate =
          new DoubleBrokerMetric(objectName, "OneMinuteRate");
    }

    public static class ReplicationBytesInPerSec {

      private static final String objectName =
          "kafka.server:type=BrokerTopicMetrics,name=ReplicationBytesInPerSec";

      public static final LongBrokerMetric count = new LongBrokerMetric(objectName, "Count");
      public static final DoubleBrokerMetric fifteenMinuteRate =
          new DoubleBrokerMetric(objectName, "FifteenMinuteRate");
      public static final DoubleBrokerMetric fiveMinuteRate =
          new DoubleBrokerMetric(objectName, "FiveMinuteRate");
      public static final DoubleBrokerMetric meanRate =
          new DoubleBrokerMetric(objectName, "MeanRate");
      public static final DoubleBrokerMetric oneMinuteRate =
          new DoubleBrokerMetric(objectName, "OneMinuteRate");
    }

    public static class ReplicationBytesOutPerSec {

      private static final String objectName =
          "kafka.server:type=BrokerTopicMetrics,name=ReplicationBytesOutPerSec";

      public static final LongBrokerMetric count = new LongBrokerMetric(objectName, "Count");
      public static final DoubleBrokerMetric fifteenMinuteRate =
          new DoubleBrokerMetric(objectName, "FifteenMinuteRate");
      public static final DoubleBrokerMetric fiveMinuteRate =
          new DoubleBrokerMetric(objectName, "FiveMinuteRate");
      public static final DoubleBrokerMetric meanRate =
          new DoubleBrokerMetric(objectName, "MeanRate");
      public static final DoubleBrokerMetric oneMinuteRate =
          new DoubleBrokerMetric(objectName, "OneMinuteRate");
    }

    public static class TotalFetchRequestsPerSec {

      private static final String objectName =
          "kafka.server:type=BrokerTopicMetrics,name=TotalFetchRequestsPerSec";

      public static final LongBrokerMetric count = new LongBrokerMetric(objectName, "Count");
      public static final DoubleBrokerMetric fifteenMinuteRate =
          new DoubleBrokerMetric(objectName, "FifteenMinuteRate");
      public static final DoubleBrokerMetric fiveMinuteRate =
          new DoubleBrokerMetric(objectName, "FiveMinuteRate");
      public static final DoubleBrokerMetric meanRate =
          new DoubleBrokerMetric(objectName, "MeanRate");
      public static final DoubleBrokerMetric oneMinuteRate =
          new DoubleBrokerMetric(objectName, "OneMinuteRate");
    }

    public static class TotalProduceRequestsPerSec {

      private static final String objectName =
          "kafka.server:type=BrokerTopicMetrics,name=TotalProduceRequestsPerSec";

      public static final LongBrokerMetric count = new LongBrokerMetric(objectName, "Count");
      public static final DoubleBrokerMetric fifteenMinuteRate =
          new DoubleBrokerMetric(objectName, "FifteenMinuteRate");
      public static final DoubleBrokerMetric fiveMinuteRate =
          new DoubleBrokerMetric(objectName, "FiveMinuteRate");
      public static final DoubleBrokerMetric meanRate =
          new DoubleBrokerMetric(objectName, "MeanRate");
      public static final DoubleBrokerMetric oneMinuteRate =
          new DoubleBrokerMetric(objectName, "OneMinuteRate");
    }
  }

  public static class RequestMetrics {

    public final MeasuredValue localTimeMs;
    public final MeasuredValue remoteTimeMs;
    public final MeasuredValue requestBytes;
    public final MeasuredValue requestQueueTimeMs;
    public final MeasuredValue responseQueueTimeMs;
    public final MeasuredValue responseSendTimeMs;
    public final MeasuredValue throttleTimeMs;
    public final MeasuredValue totalTimeMs;

    private RequestMetrics(String requestName) {
      this.localTimeMs = new MeasuredValue(requestName, "LocalTimeMs");
      this.remoteTimeMs = new MeasuredValue(requestName, "RemoteTimeMs");
      this.requestBytes = new MeasuredValue(requestName, "RequestBytes");
      this.requestQueueTimeMs = new MeasuredValue(requestName, "RequestQueueTimeMs");
      this.responseQueueTimeMs = new MeasuredValue(requestName, "ResponseQueueTimeMs");
      this.responseSendTimeMs = new MeasuredValue(requestName, "ResponseSendTimeMs");
      this.throttleTimeMs = new MeasuredValue(requestName, "ThrottleTimeMs");
      this.totalTimeMs = new MeasuredValue(requestName, "TotalTimeMs");
    }

    public static RequestMetrics of(String requestName) {
      return new RequestMetrics(requestName);
    }

    public static class MeasuredValue implements StatisticsMetricsTemplate {

      private String formattedJmxName;
      public final String requestName;
      public final String measurement;

      public MeasuredValue(String requestName, String measurement) {
        this.requestName = requestName;
        this.measurement = measurement;
      }

      @Override
      public String createJmxName() {
        if (formattedJmxName == null) {
          this.formattedJmxName =
              String.format(
                  "kafka.network:type=RequestMetrics,request=%s,name=%s", requestName, measurement);
        }
        return formattedJmxName;
      }
    }
  }

  public static class ProcessorThread {

    public final DoubleBrokerMetric idlePercent;

    private ProcessorThread(int threadId) {
      final String objectNameFormat =
          "kafka.network:type=Processor,networkProcessor=%d,name=IdlePercent";
      final String objectName = String.format(objectNameFormat, threadId);
      this.idlePercent = new DoubleBrokerMetric(objectName, "Value");
    }

    public static ProcessorThread of(int threadId) {
      // FIX: below code will introduce massive amount of redundant objects under frequently
      // calling, beware of that.
      return new ProcessorThread(threadId);
    }
  }

  public static class JvmMemory {
    public static final CustomCompositeDataMetric<MemoryUsage> heapMemoryUsage =
        new CustomCompositeDataMetric<>(
            "java.lang:type=Memory", "HeapMemoryUsage", MemoryUsage::from);
    public static final CustomCompositeDataMetric<MemoryUsage> nonHeapMemoryUsage =
        new CustomCompositeDataMetric<>(
            "java.lang:type=Memory", "NonHeapMemoryUsage", MemoryUsage::from);
  }

  public static class JvmOperatingSystem {

    private static final String objectName = "java.lang:type=OperatingSystem";

    public static final StringBrokerMetric arch = new StringBrokerMetric(objectName, "Arch");
    public static final StringBrokerMetric name = new StringBrokerMetric(objectName, "Name");
    public static final StringBrokerMetric version = new StringBrokerMetric(objectName, "Version");

    public static final IntegerBrokerMetric availableProcessors =
        new IntegerBrokerMetric(objectName, "AvailableProcessors");
    public static final LongBrokerMetric committedVirtualMemorySize =
        new LongBrokerMetric(objectName, "CommittedVirtualMemorySize");
    public static final LongBrokerMetric freePhysicalMemorySize =
        new LongBrokerMetric(objectName, "FreePhysicalMemorySize");
    public static final LongBrokerMetric freeSwapSpaceSize =
        new LongBrokerMetric(objectName, "FreeSwapSpaceSize");
    public static final LongBrokerMetric maxFileDescriptorCount =
        new LongBrokerMetric(objectName, "MaxFileDescriptorCount");
    public static final LongBrokerMetric openFileDescriptorCount =
        new LongBrokerMetric(objectName, "OpenFileDescriptorCount");
    public static final DoubleBrokerMetric ProcessCpuLoad =
        new DoubleBrokerMetric(objectName, "ProcessCpuLoad");
    public static final LongBrokerMetric ProcessCpuTime =
        new LongBrokerMetric(objectName, "ProcessCpuTime");
    public static final DoubleBrokerMetric systemCpuLoad =
        new DoubleBrokerMetric(objectName, "SystemCpuLoad");
    public static final DoubleBrokerMetric systemLoadAverage =
        new DoubleBrokerMetric(objectName, "SystemLoadAverage");
    public static final LongBrokerMetric totalPhysicalMemorySize =
        new LongBrokerMetric(objectName, "TotalPhysicalMemorySize");
    public static final LongBrokerMetric totalSwapSpaceSize =
        new LongBrokerMetric(objectName, "TotalSwapSpaceSize");
  }

  public static class SocketServer {
    public static final DoubleBrokerMetric networkProcessorAvgIdlePercent =
        new DoubleBrokerMetric(
            "kafka.network:type=SocketServer,name=NetworkProcessorAvgIdlePercent", "Value");
  }

  public static class Disk {

    public static final LongBrokerMetric linuxDiskReadBytes =
        new LongBrokerMetric("kafka.server:type=KafkaServer,name=linux-disk-read-bytes", "Value");
    public static final LongBrokerMetric linuxDiskWriteBytes =
        new LongBrokerMetric("kafka.server:type=KafkaServer,name=linux-disk-write-bytes", "Value");
  }

  public static class RequestHandler {

    public static class RequestHandlerAvgIdlePercent {

      private static final String objectName =
          "kafka.server:type=KafkaRequestHandlerPool,name=RequestHandlerAvgIdlePercent";

      public static final LongBrokerMetric count = new LongBrokerMetric(objectName, "Count");
      public static final DoubleBrokerMetric fifteenMinuteRate =
          new DoubleBrokerMetric(objectName, "FifteenMinuteRate");
      public static final DoubleBrokerMetric fiveMinuteRate =
          new DoubleBrokerMetric(objectName, "FiveMinuteRate");
      public static final DoubleBrokerMetric meanRate =
          new DoubleBrokerMetric(objectName, "MeanRate");
      public static final DoubleBrokerMetric oneMinuteRate =
          new DoubleBrokerMetric(objectName, "OneMinuteRate");
    }
  }
}
