package org.astraea.partitioner.nodeLoadMetric;

import static org.astraea.partitioner.nodeLoadMetric.PartitionerUtils.allPoisson;
import static org.astraea.partitioner.nodeLoadMetric.PartitionerUtils.doPoisson;
import static org.astraea.partitioner.nodeLoadMetric.PartitionerUtils.factorial;
import static org.astraea.partitioner.nodeLoadMetric.PartitionerUtils.parseIdJMXPort;
import static org.astraea.partitioner.nodeLoadMetric.PartitionerUtils.weightPoisson;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.Map;
import org.astraea.partitioner.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PartitionerUtilsTest {

  @Test
  void testFactorial() {
    assertEquals(factorial(3), 6);
    assertEquals(factorial(5), 120);
  }

  @Test
  void testDoPoisson() {
    assertEquals(doPoisson(10, 15), 0.9512595966960214);
    assertEquals(doPoisson(5, 5), 0.6159606548330632);
    assertEquals(doPoisson(5, 8), 0.9319063652781515);
    assertEquals(doPoisson(5, 2), 0.12465201948308113);
  }

  @Test
  void testSetAllPoisson() {
    HashMap<Integer, Integer> testNodesLoadCount = new HashMap<>();
    testNodesLoadCount.put(0, 2);
    testNodesLoadCount.put(1, 5);
    testNodesLoadCount.put(2, 8);

    Map<Integer, Double> poissonMap;
    HashMap<Integer, Double> testPoissonMap = new HashMap<>();
    testPoissonMap.put(0, 0.12465201948308113);
    testPoissonMap.put(1, 0.6159606548330632);
    testPoissonMap.put(2, 0.9319063652781515);

    poissonMap = allPoisson(testNodesLoadCount);

    assertEquals(poissonMap, testPoissonMap);
  }

  @Test
  void testWeightPoisson() {
    Assertions.assertEquals(weightPoisson(0.5, 1.0), 10);
    Assertions.assertEquals(weightPoisson(1.0, 1.0), 0);
    Assertions.assertEquals(weightPoisson(0.95, 1.0), 0);
    Assertions.assertEquals(weightPoisson(0.9, 1.0), 0);
    Assertions.assertEquals(weightPoisson(0.0, 1.0), 20);
    Assertions.assertEquals(weightPoisson(0.8, 1.0), 1);
  }

  @Test
  void testParseIdJMXPort() {
    var config =
        Configuration.of(Map.of("broker.1001.jmx.port", "8000", "broker.1002.jmx.port", "8001"));
    var ans = parseIdJMXPort(config);
    Assertions.assertEquals(2, ans.size());
    Assertions.assertEquals(8000, ans.get(1001));
    Assertions.assertEquals(8001, ans.get(1002));

    config = Configuration.of(Map.of("jmx.port", "8000", "broker.1002.jmx.port", "8001"));
    ans = parseIdJMXPort(config);
    Assertions.assertEquals(1, ans.size());
    Assertions.assertEquals(8001, ans.get(1002));

    var config3 = Configuration.of(Map.of("broker.id.jmx.port", "8000"));
    Assertions.assertThrows(NumberFormatException.class, () -> parseIdJMXPort(config3));
  }
}
