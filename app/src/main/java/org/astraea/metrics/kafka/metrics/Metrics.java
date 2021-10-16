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

    /**
     * Number of requests waiting in the producer purgatory.
     *
     * @param request fetch specific purgatory related to this request.
     * @return Number of requests waiting in the producer purgatory.
     */
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

    /**
     * Request rate.
     *
     * @param request the specific request to fetch.
     * @return the request rate of specific request.
     */
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

    /**
     * Number of partitions across all topics in the cluster.
     *
     * @return number of partitions across all topics in the cluster.
     */
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

    /**
     * Number of under-replicated partitions (| ISR | < | current replicas |). Replicas that are
     * added as part of a reassignment will not count toward this value. Alert if value is greater
     * than 0.
     *
     * @return number of under-replicated partitions.
     */
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
