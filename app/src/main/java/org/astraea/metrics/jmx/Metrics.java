package org.astraea.metrics.jmx;
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
  }
}
