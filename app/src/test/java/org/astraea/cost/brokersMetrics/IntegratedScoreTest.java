package org.astraea.cost.brokersMetrics;

import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class IntegratedScoreTest {
  @Test
  void testEntropyEmpowerment() {
    var neutralIntegratedCost = new NeutralIntegratedCost();
    var brokers =
        Map.of(
            0,
            new NeutralIntegratedCost.BrokerMetrics(),
            1,
            new NeutralIntegratedCost.BrokerMetrics(),
            2,
            new NeutralIntegratedCost.BrokerMetrics());
    brokers.get(0).cpuScore = 0.1252;
    brokers.get(0).inputScore = 550.0;
    brokers.get(0).outputScore = 545.0;
    brokers.get(0).memoryScore = 0.254;

    brokers.get(1).inputScore = 750.0;
    brokers.get(1).outputScore = 755.0;
    brokers.get(1).memoryScore = 0.268;
    brokers.get(1).cpuScore = 0.1342;

    brokers.get(2).inputScore = 550.0;
    brokers.get(2).outputScore = 550.0;
    brokers.get(2).memoryScore = 0.235;
    brokers.get(2).cpuScore = 0.1522;

    var metrics = neutralIntegratedCost.entropyEmpowerment().empowerment(brokers);

    Assertions.assertEquals(0.46689278143812796, metrics.get("inputThroughput"));
    Assertions.assertEquals(0.4200836938888968, metrics.get("outputThroughput"));
    Assertions.assertEquals(0.05781512136314425, metrics.get("memory"));
    Assertions.assertEquals(0.055208403309831, metrics.get("cpu"));
  }
}
