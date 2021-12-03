// package org.astraea.partitioner.nodeLoadMetric;
//
// import static java.lang.Double.sum;
//
// import java.util.ArrayList;
// import java.util.HashMap;
// import java.util.List;
// import java.util.Map;
// import java.util.Objects;
// import java.util.Set;
// import java.util.concurrent.ConcurrentHashMap;
// import java.util.stream.Collectors;
// import java.util.stream.IntStream;
//
// import org.astraea.metrics.BeanCollector;
// import org.astraea.metrics.HasBeanObject;
//
// import org.astraea.metrics.kafka.KafkaMetrics;
//
// public class OverLoadNode {
//  private final BeanCollector beanCollector;
//  private Map<Map.Entry<String, Integer>, Map<String, List<HasBeanObject>>> valueMetrics;
//
//  OverLoadNode(BeanCollector collector) {
//    beanCollector = collector;
//  }
//
//  /** Monitor and update the number of overloads of each node. */
//  public Map<Map.Entry<String, Integer>, Integer> nodesOverLoad(
//          Set<Map.Entry<String, Integer>> addresses) {
//    var nodesMetrics = beanCollector.objects();
//    valueMetrics = new ConcurrentHashMap<>();
//    for (Map.Entry<String, Integer> address : addresses) {
//      var entrySet =
//              nodesMetrics.entrySet().stream()
//                      .filter(entry -> Objects.equals(entry.getKey(), address))
//                      .findFirst()
//                      .get();
//      valueMetrics.put(entrySet.getKey(), entrySet.getValue());
//    }
//
//    var eachBrokerMsgPerSec = brokersMsgPerSec();
//
//    var overLoadCount = new HashMap<Map.Entry<String, Integer>, Integer>();
//    eachBrokerMsgPerSec.keySet().forEach(e -> overLoadCount.put(e, 0));
//    var i = 0;
//    while (i < 10) {
//      var avg = getAvgMsgSec(i);
//      var eachMsg = getBrokersMsgSec(i);
//      var standardDeviation = standardDeviationImperative(eachMsg, avg);
//      for (Map.Entry<Map.Entry<String, Integer>, Double> entry : eachMsg.entrySet()) {
//        if (entry.getValue() > (avg + standardDeviation)) {
//          overLoadCount.put(entry.getKey(), overLoadCount.get(entry.getKey()) + 1);
//        }
//      }
//      i++;
//    }
//    return overLoadCount;
//  }
//
//  private Map<Map.Entry<String, Integer>, Double> getBrokersMsgSec(int index) {
//    return brokersMsgPerSec().entrySet().stream()
//            .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().get(index)));
//  }
//
//  private double getAvgMsgSec(int index) {
//    return avgBrokersMsgPerSec(brokersMsgPerSec()).get(index);
//  }
//
//  private Map<Map.Entry<String, Integer>, List<Double>> brokersMsgPerSec() {
//    var eachMsg = new HashMap<Map.Entry<String, Integer>, List<Double>>();
//
//    for (Map.Entry<Map.Entry<String, Integer>, Map<String, List<HasBeanObject>>> entry :
//            valueMetrics.entrySet()) {
//      List<Double> sumList = new ArrayList<>();
//      var outMsg = entry.getValue().get(KafkaMetrics.BrokerTopic.BytesOutPerSec.toString());
//      var inMsg = entry.getValue().get(KafkaMetrics.BrokerTopic.BytesInPerSec.toString());
//
//      IntStream.range(
//                      0, valueMetrics.values().stream().map(s ->
// s.values()).findFirst().get().size())
//              .mapToObj(
//                      i ->
//                              sumList.add(
//                                      sum(
//                                              Double.parseDouble(
//                                                      outMsg
//                                                              .get(i)
//                                                              .beanObject()
//                                                              .getAttributes()
//                                                              .get("MeanRate")
//                                                              .toString()),
//                                              Double.parseDouble(
//                                                      inMsg
//                                                              .get(i)
//                                                              .beanObject()
//                                                              .getAttributes()
//                                                              .get("MeanRate")
//                                                              .toString()))));
//
//      eachMsg.put(entry.getKey(), sumList);
//    }
//    return eachMsg;
//  }
//
//  public List<Double> avgBrokersMsgPerSec(Map<Map.Entry<String, Integer>, List<Double>> eachMsg) {
//
//    return IntStream.range(0, eachMsg.values().stream().findFirst().get().size())
//            .mapToDouble(i -> eachMsg.values().stream().map(s -> s.get(i)).reduce(0.0,
// Double::sum))
//            .map(sum -> sum / eachMsg.size())
//            .boxed()
//            .collect(Collectors.toList());
//  }
//
//  public double standardDeviationImperative(
//          Map<Map.Entry<String, Integer>, Double> eachMsg, double avgBrokersMsgPerSec) {
//    var variance = 0.0;
//    for (Map.Entry<Map.Entry<String, Integer>, Double> entry : eachMsg.entrySet()) {
//      variance +=
//              (entry.getValue() - avgBrokersMsgPerSec) * (entry.getValue() - avgBrokersMsgPerSec);
//    }
//    return Math.sqrt(variance / eachMsg.size());
//  }
// }
