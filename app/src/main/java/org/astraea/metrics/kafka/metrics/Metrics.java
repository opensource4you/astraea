package org.astraea.metrics.kafka.metrics;

import java.util.List;
import org.astraea.metrics.jmx.BeanObject;
import org.astraea.metrics.jmx.BeanQuery;

public final class Metrics {

  private Metrics() {}

  public static Metric<BrokerTopicMetricsResult> brokerTopicMetric(BrokerTopicMetrics m) {
    return m;
  }

  public static final class Purgatory {

    private Purgatory() {}

    public static Metric<Integer> size(PurgatoryRequest request) {
      return new Metric<>() {
        @Override
        public List<BeanQuery> queries() {
          return List.of(
              BeanQuery.builder("kafka.server")
                  .property("type", "DelayedOperationPurgatory")
                  .property("delayedOperation", request.name())
                  .property("name", "PurgatorySize")
                  .build());
        }

        @Override
        public Integer from(List<BeanObject> beanObject) {
          return (Integer) beanObject.get(0).getAttributes().get("Value");
        }
      };
    }
  }

  public static final class RequestMetrics {

    private RequestMetrics() {}

    public static Metric<TotalTimeMs> totalTimeMs(RequestTotalTimeMs request) {
      return new Metric<>() {
        @Override
        public List<BeanQuery> queries() {
          return List.of(
              BeanQuery.builder("kafka.network")
                  .property("type", "RequestMetrics")
                  .property("request", request.name())
                  .property("name", "TotalTimeMs")
                  .build());
        }

        @Override
        public TotalTimeMs from(List<BeanObject> beanObjects) {
          return new TotalTimeMs(beanObjects.get(0));
        }
      };
    }

    public enum RequestTotalTimeMs {
      Produce,
      FetchConsumer,
      FetchFollower
    }
  }

  public static final class TopicPartition {

    private TopicPartition() {}

    public static Metric<Integer> globalPartitionCount() {
      return new Metric<>() {
        @Override
        public List<BeanQuery> queries() {
          return List.of(
              BeanQuery.builder("kafka.controller")
                  .property("type", "KafkaController")
                  .property("name", "GlobalPartitionCount")
                  .build());
        }

        @Override
        public Integer from(List<BeanObject> beanObject) {
          return (Integer) beanObject.get(0).getAttributes().get("Value");
        }
      };
    }

    public static Metric<Integer> underReplicatedPartitions() {
      return new Metric<>() {
        @Override
        public List<BeanQuery> queries() {
          return List.of(
              BeanQuery.builder("kafka.server")
                  .property("type", "ReplicaManager")
                  .property("name", "UnderReplicatedPartitions")
                  .build());
        }

        @Override
        public Integer from(List<BeanObject> beanObject) {
          return (Integer) beanObject.get(0).getAttributes().get("Value");
        }
      };
    }
  }
}
