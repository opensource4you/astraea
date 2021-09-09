package org.astraea.metrics.jmx;

import java.lang.management.MemoryUsage;

/** A list of all kafka metrics */
public final class Metrics {

  // This class intent to use as a collection of kafka metric object.
  // The following private construction, making this class impossible to initiate.
  private Metrics() {}

  public static class BrokerTopic {
    public static class MessageInPerSecond {
      public static final LongBrokerMetric count =
          new LongBrokerMetric(
              "kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec", "Count");
      public static final DoubleBrokerMetric fifteenMinuteRate =
          new DoubleBrokerMetric(
              "kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec", "FifteenMinuteRate");
      public static final DoubleBrokerMetric fiveMinuteRate =
          new DoubleBrokerMetric(
              "kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec", "FiveMinuteRate");
      public static final DoubleBrokerMetric meanRate =
          new DoubleBrokerMetric(
              "kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec", "MeanRate");
      public static final DoubleBrokerMetric oneMinuteRate =
          new DoubleBrokerMetric(
              "kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec", "OneMinuteRate");
    }

    public static class BytesInPerSecond {
      public static final LongBrokerMetric count =
          new LongBrokerMetric("kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec", "Count");
      public static final DoubleBrokerMetric fifteenMinuteRate =
          new DoubleBrokerMetric(
              "kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec", "FifteenMinuteRate");
      public static final DoubleBrokerMetric fiveMinuteRate =
          new DoubleBrokerMetric(
              "kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec", "FiveMinuteRate");
      public static final DoubleBrokerMetric meanRate =
          new DoubleBrokerMetric(
              "kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec", "MeanRate");
      public static final DoubleBrokerMetric oneMinuteRate =
          new DoubleBrokerMetric(
              "kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec", "OneMinuteRate");
    }

    public static class BytesOutPerSecond {
      public static final LongBrokerMetric count =
          new LongBrokerMetric("kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec", "Count");
      public static final DoubleBrokerMetric fifteenMinuteRate =
          new DoubleBrokerMetric(
              "kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec", "FifteenMinuteRate");
      public static final DoubleBrokerMetric fiveMinuteRate =
          new DoubleBrokerMetric(
              "kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec", "FiveMinuteRate");
      public static final DoubleBrokerMetric meanRate =
          new DoubleBrokerMetric(
              "kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec", "MeanRate");
      public static final DoubleBrokerMetric oneMinuteRate =
          new DoubleBrokerMetric(
              "kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec", "OneMinuteRate");
    }

    public static class ReplicationBytesInPerSec {
      public static final LongBrokerMetric count =
          new LongBrokerMetric(
              "kafka.server:type=BrokerTopicMetrics,name=ReplicationBytesInPerSec", "Count");
      public static final DoubleBrokerMetric fifteenMinuteRate =
          new DoubleBrokerMetric(
              "kafka.server:type=BrokerTopicMetrics,name=ReplicationBytesInPerSec",
              "FifteenMinuteRate");
      public static final DoubleBrokerMetric fiveMinuteRate =
          new DoubleBrokerMetric(
              "kafka.server:type=BrokerTopicMetrics,name=ReplicationBytesInPerSec",
              "FiveMinuteRate");
      public static final DoubleBrokerMetric meanRate =
          new DoubleBrokerMetric(
              "kafka.server:type=BrokerTopicMetrics,name=ReplicationBytesInPerSec", "MeanRate");
      public static final DoubleBrokerMetric oneMinuteRate =
          new DoubleBrokerMetric(
              "kafka.server:type=BrokerTopicMetrics,name=ReplicationBytesInPerSec",
              "OneMinuteRate");
    }

    public static class ReplicationBytesOutPerSec {
      public static final LongBrokerMetric count =
          new LongBrokerMetric(
              "kafka.server:type=BrokerTopicMetrics,name=ReplicationBytesOutPerSec", "Count");
      public static final DoubleBrokerMetric fifteenMinuteRate =
          new DoubleBrokerMetric(
              "kafka.server:type=BrokerTopicMetrics,name=ReplicationBytesOutPerSec",
              "FifteenMinuteRate");
      public static final DoubleBrokerMetric fiveMinuteRate =
          new DoubleBrokerMetric(
              "kafka.server:type=BrokerTopicMetrics,name=ReplicationBytesOutPerSec",
              "FiveMinuteRate");
      public static final DoubleBrokerMetric meanRate =
          new DoubleBrokerMetric(
              "kafka.server:type=BrokerTopicMetrics,name=ReplicationBytesOutPerSec", "MeanRate");
      public static final DoubleBrokerMetric oneMinuteRate =
          new DoubleBrokerMetric(
              "kafka.server:type=BrokerTopicMetrics,name=ReplicationBytesOutPerSec",
              "OneMinuteRate");
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

  public static class SocketServer {
    public static final DoubleBrokerMetric networkProcessorAvgIdlePercent =
        new DoubleBrokerMetric(
            "kafka.network:type=SocketServer,name=NetworkProcessorAvgIdlePercent", "Value");
  }
}
