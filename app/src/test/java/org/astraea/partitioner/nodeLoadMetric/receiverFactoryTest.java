package org.astraea.partitioner.nodeLoadMetric;

import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.astraea.partitioner.smoothPartitioner.SmoothWeightPartitioner;
import org.astraea.service.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class receiverFactoryTest extends RequireBrokerCluster {
  public Properties initProConfig() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "id1");
    props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, SmoothWeightPartitioner.class.getName());
    props.put("jmx_servers", jmxServiceURL());
    return props;
  }

  @Test
  void testSingleton() {
    initProConfig();
    var jmxAddress = Map.of(jmxServiceURL().getHost(), jmxServiceURL().getPort());
    var FACTORY = new ReceiverFactory();
    var bc1 = FACTORY.receiversList(jmxAddress);
    var bc2 = FACTORY.receiversList(jmxAddress);
    Assertions.assertEquals(bc1, bc2);
    var bc3 = FACTORY.receiversList(jmxAddress);
    var bc4 = FACTORY.receiversList(jmxAddress);
    Assertions.assertEquals(FACTORY.factoryCount(jmxAddress), 4);
    FACTORY.close(jmxAddress);
    Assertions.assertEquals(FACTORY.factoryCount(jmxAddress), 3);
    FACTORY.close(jmxAddress);
    FACTORY.close(jmxAddress);
    Assertions.assertEquals(FACTORY.factoryCount(jmxAddress), 1);
    FACTORY.close(jmxAddress);
    Assertions.assertEquals(FACTORY.factoryCount(jmxAddress), 0);
    var bc5 = FACTORY.receiversList(jmxAddress);
    Assertions.assertEquals(FACTORY.factoryCount(jmxAddress), 1);
    FACTORY.close(jmxAddress);
    Assertions.assertEquals(FACTORY.factoryCount(jmxAddress), 0);
  }
}
