package org.astraea.partitioner.nodeLoadMetric;

import static org.mockito.Mockito.mockConstruction;

import java.util.Comparator;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.astraea.metrics.BeanCollector;
import org.astraea.partitioner.partitionerFactory.SmoothWeightPartitioner;
import org.astraea.service.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

public class BeanCollectorFactoryTest extends RequireBrokerCluster {

  public Properties initProConfig() {
    var props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "id1");
    props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, SmoothWeightPartitioner.class.getName());
    return props;
  }

  @Test
  void testSingleton() {
    var FACTORY =
        new BeanCollectorFactory(
            Comparator.comparing(o -> o.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG).toString()));
    try (MockedConstruction mocked = mockConstruction(BeanCollector.class)) {
      Map props = initProConfig();
      var bc1 = FACTORY.getOrCreate(props);
      var bc2 = FACTORY.getOrCreate(props);
      Assertions.assertEquals(bc1, bc2);
      var bc3 = FACTORY.getOrCreate(props);
      Assertions.assertEquals(FACTORY.factoryCount().get(props), 3);

      FACTORY.close(props);
      FACTORY.close(props);
      Assertions.assertNotNull(FACTORY.Instances().get(props));
      FACTORY.close(props);
      Assertions.assertNull(FACTORY.Instances().get(props));
    }
  }
}
