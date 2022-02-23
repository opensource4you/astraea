package org.astraea.partitioner;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.astraea.partitioner.nodeLoadMetric.NodeLoadClient;

public class LoadPartitioner implements Partitioner {
  private NodeLoadClient nodeLoadClient;
  private AstraeaPartitioner astraeaPartitioner;

  @Override
  public int partition(
      String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
    return astraeaPartitioner.loadPartition(topic, key, keyBytes, value, valueBytes, cluster);
  }

  @Override
  public void close() {
    nodeLoadClient.close();
  }

  @Override
  public void configure(Map<String, ?> configs) {
    try {
      var jmxAddresses =
          Objects.requireNonNull(
              configs.get("jmx_servers").toString(), "You must configure jmx_servers correctly");
      var list = Arrays.asList((jmxAddresses).split(","));
      HashMap<String, Integer> mapAddress = new HashMap<>();
      list.forEach(
          str ->
              mapAddress.put(
                  Arrays.asList(str.split(":")).get(0),
                  Integer.parseInt(Arrays.asList(str.split(":")).get(1))));
      Objects.requireNonNull(mapAddress, "You must configure jmx_servers correctly.");
      nodeLoadClient = new NodeLoadClient(mapAddress);
      var partitioner = "flexible";
      if (configs.containsKey("partition_method")) {
        partitioner = (String) configs.get("partition_method");
      }
      switch (partitioner) {
        case ("flexible"):
          astraeaPartitioner = new FlexibleRoundRobinPartitioner(nodeLoadClient);
        case ("smooth"):
          astraeaPartitioner = new SmoothWeightPartitioner(nodeLoadClient);
      }
    } catch (IOException e) {
      throw new RuntimeException();
    }
  }
}
