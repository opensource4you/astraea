package org.astraea.partitioner.smoothPartitioner;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.astraea.metrics.HasBeanObject;
import org.astraea.metrics.java.HasJvmMemory;
import org.astraea.metrics.java.OperatingSystemInfo;
import org.astraea.metrics.kafka.BrokerTopicMetricsResult;
import org.astraea.metrics.kafka.KafkaMetrics;

public class DynamicWeightsMetrics {
  private final Map<Integer, BrokerMetric> brokersMetric = new HashMap<>();

  public enum BrokerMetrics {
    inputThroughput("inputThroughput"),
    outputThroughput("outputThroughput"),
    jvm("jvm"),
    cpu("cpu");

    private final String metricName;

    BrokerMetrics(String name) {
      this.metricName = name;
    }

    public String metricName() {
      return metricName;
    }
  }

  public void updateMetrics(Map<Integer, Collection<HasBeanObject>> allBeans) {
    allBeans.forEach(
        (brokerID, value) -> {
          if (!brokersMetric.containsKey(brokerID))
            brokersMetric.put(brokerID, new BrokerMetric(brokerID));
          value.forEach(
              result -> {
                //                System.out.println("3.0" + result.getClass());
                if (result instanceof HasJvmMemory) {
                  //                  System.out.println("jvm");
                  var jvmBean = (HasJvmMemory) result;
                  brokersMetric.get(brokerID).jvmUsage =
                      jvmBean.heapMemoryUsage().getUsed()
                          / (jvmBean.heapMemoryUsage().getMax() + 1.0);
                } else if (result instanceof OperatingSystemInfo) {
                  var cpuBean = (OperatingSystemInfo) result;
                  brokersMetric.get(brokerID).cpuUsage = cpuBean.systemCpuLoad();
                } else if (result
                    .beanObject()
                    .getProperties()
                    .get("name")
                    .equals(KafkaMetrics.BrokerTopic.BytesInPerSec.metricName())) {
                  var inBean = (BrokerTopicMetricsResult) result;
                  //                  System.out.println(brokerID + ":" + inBean.count());
                  brokersMetric.get(brokerID).inputCount =
                      inBean.count() - brokersMetric.get(brokerID).accumulateInputCount;
                  brokersMetric.get(brokerID).accumulateInputCount = inBean.count();
                } else if (result
                    .beanObject()
                    .getProperties()
                    .get("name")
                    .equals(KafkaMetrics.BrokerTopic.BytesOutPerSec.metricName())) {
                  var outBean = (BrokerTopicMetricsResult) result;
                  brokersMetric.get(brokerID).outputCount =
                      outBean.count() - brokersMetric.get(brokerID).accumulateOutputCount;
                  brokersMetric.get(brokerID).accumulateOutputCount = outBean.count();
                }
              });
        });
    //    System.out.println(inputCount());
    //    System.out.println(jvmUsage());
    //    System.out.println(cpuUsage());
  }

  public Map<Integer, Double> inputCount() {
    return brokersMetric.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().inputCount));
  }

  public Map<Integer, Double> outputCount() {
    return brokersMetric.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().outputCount));
  }

  public Map<Integer, Double> jvmUsage() {
    return brokersMetric.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().jvmUsage));
  }

  public Map<Integer, Double> cpuUsage() {
    return brokersMetric.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().cpuUsage));
  }

  private static class BrokerMetric {
    private final int brokerID;

    private double inputCount = 0.0;
    private Long accumulateInputCount = 0L;
    private double outputCount = 0.0;
    private Long accumulateOutputCount = 0L;
    private double jvmUsage = 0.0;
    private double cpuUsage = 0.0;

    BrokerMetric(int brokerID) {
      this.brokerID = brokerID;
    }
  }
}
