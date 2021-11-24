package org.astraea.partitioner.nodeLoadMetric;

import static java.util.Arrays.asList;
import static org.astraea.partitioner.partitionerFactory.SmoothWeightPartitioner.getFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.astraea.partitioner.partitionerFactory.SmoothWeightPartitioner;
import org.astraea.producer.Serializer;
import org.astraea.service.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class NodeLoadClientFactoryTest extends RequireBrokerCluster {
  private String jmxAddresses;

  private static final Node[] NODES =
      new Node[] {
        new Node(0, "localhost", 99), new Node(1, "localhost", 100), new Node(12, "localhost", 101)
      };
  private static final String TOPIC = "test";
  // Intentionally make the partition list not in partition order to test the edge cases.
  private static final List<PartitionInfo> PARTITIONS =
      asList(
          new PartitionInfo(TOPIC, 1, null, NODES, NODES),
          new PartitionInfo(TOPIC, 2, NODES[1], NODES, NODES),
          new PartitionInfo(TOPIC, 0, NODES[0], NODES, NODES));

  @BeforeEach
  void reset() {
    jmxAddresses = "0.0.0.0,1.1.1.1,2.2.2.2";
  }

  public Properties initProConfig() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "id1");
    props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, SmoothWeightPartitioner.class.getName());
    props.put("jmx_servers", jmxServiceURL() + "@0");
    return props;
  }

  @Test
  void testSingletonByNodeLoadClient() {

    var props = initProConfig();
    // create multiples partitioners
    var producers =
        IntStream.range(0, 10)
            .mapToObj(
                i ->
                    org.astraea.producer.Producer.builder()
                        .keySerializer(Serializer.STRING)
                        .valueSerializer(Serializer.STRING)
                        .configs(
                            props.entrySet().stream()
                                .collect(
                                    Collectors.toMap(
                                        e -> e.getKey().toString(), Map.Entry::getValue)))
                        .build())
            .collect(Collectors.toList());

    Assertions.assertEquals(getFactory().getnodeLoadClientMap().size(), 1);
    producers.forEach(org.astraea.producer.Producer::close);
    Assertions.assertNull(getFactory().getnodeLoadClientMap().get(props));

    Partitioner partitioner1 = new SmoothWeightPartitioner();
    partitioner1.configure(
        props.entrySet().stream()
            .collect(Collectors.toMap(e -> e.getKey().toString(), Map.Entry::getValue)));
    Partitioner partitioner2 = new SmoothWeightPartitioner();
    partitioner2.configure(
        props.entrySet().stream()
            .collect(Collectors.toMap(e -> e.getKey().toString(), Map.Entry::getValue)));
    Partitioner partitioner3 = new SmoothWeightPartitioner();
    partitioner3.configure(
        props.entrySet().stream()
            .collect(Collectors.toMap(e -> e.getKey().toString(), Map.Entry::getValue)));

    Assertions.assertEquals(getFactory().getnodeLoadClientMap().size(), 1);
    partitioner1.close();
    partitioner2.close();
    Assertions.assertEquals(getFactory().getnodeLoadClientMap().size(), 1);
    partitioner3.close();
    Assertions.assertNull(getFactory().getnodeLoadClientMap().get(props));
  }

  @Test
  void testThrowException() {
    var configs =
        Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:11111", "jmx_server", jmxAddresses);

    SmoothWeightPartitioner partitioner = new SmoothWeightPartitioner();
    Assertions.assertThrows(RuntimeException.class, () -> partitioner.configure(configs))
        .getMessage();
  }
}
