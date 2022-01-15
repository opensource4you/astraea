package org.astraea.performance;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.astraea.Utils;

/**
 * This partitioner configures the specify_broker parameter to specify the broker that it wants to
 * send data to.
 */
public class SpecifyBrokerPartitioner implements Partitioner {

  private Map<String, Integer> hostPort;
  private List<Integer> nodesID;

  @Override
  public int partition(
      String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
    if (nodesID == null) {
      nodesID =
          cluster.nodes().stream()
              .filter(this::hasAddress)
              .map(node -> node.id())
              .collect(Collectors.toList());
    }
    var partitions = cluster.partitionsForNode((int) (Math.random() * nodesID.size()));
    return partitions.get((int) (Math.random() * partitions.size())).partition();
  }

  @Override
  public void close() {}

  @Override
  public void configure(Map<String, ?> configs) {
    var specifyBroker =
        Objects.requireNonNull(
            configs.get("specify_broker").toString(), "You must configure jmx_servers correctly");
    var list = Arrays.asList((specifyBroker).split(","));
    hostPort = Utils.requireHostPort(list);
  }

  private boolean hasAddress(Node node) {
    return hostPort.containsKey(node.host()) && hostPort.containsValue(node.port());
  }
}
