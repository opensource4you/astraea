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
          new LongBrokerMetric(
              "kafka.server:type=BrokerTopicMetrics,name=BytesInPerSecond", "Count");
      public static final DoubleBrokerMetric fifteenMinuteRate =
          new DoubleBrokerMetric(
              "kafka.server:type=BrokerTopicMetrics,name=BytesInPerSecond", "FifteenMinuteRate");
      public static final DoubleBrokerMetric fiveMinuteRate =
          new DoubleBrokerMetric(
              "kafka.server:type=BrokerTopicMetrics,name=BytesInPerSecond", "FiveMinuteRate");
      public static final DoubleBrokerMetric meanRate =
          new DoubleBrokerMetric(
              "kafka.server:type=BrokerTopicMetrics,name=BytesInPerSecond", "MeanRate");
      public static final DoubleBrokerMetric oneMinuteRate =
          new DoubleBrokerMetric(
              "kafka.server:type=BrokerTopicMetrics,name=BytesInPerSecond", "OneMinuteRate");
    }

    public static class ReplicationBytesInPerSec {
      public static final LongBrokerMetric count =
              new LongBrokerMetric(
                      "kafka.server:type=BrokerTopicMetrics,name=ReplicationBytesInPerSec", "Count");
      public static final DoubleBrokerMetric fifteenMinuteRate =
              new DoubleBrokerMetric(
                      "kafka.server:type=BrokerTopicMetrics,name=ReplicationBytesInPerSec", "FifteenMinuteRate");
      public static final DoubleBrokerMetric fiveMinuteRate =
              new DoubleBrokerMetric(
                      "kafka.server:type=BrokerTopicMetrics,name=ReplicationBytesInPerSec", "FiveMinuteRate");
      public static final DoubleBrokerMetric meanRate =
              new DoubleBrokerMetric(
                      "kafka.server:type=BrokerTopicMetrics,name=ReplicationBytesInPerSec", "MeanRate");
      public static final DoubleBrokerMetric oneMinuteRate =
              new DoubleBrokerMetric(
                      "kafka.server:type=BrokerTopicMetrics,name=ReplicationBytesInPerSec", "OneMinuteRate");
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

}
