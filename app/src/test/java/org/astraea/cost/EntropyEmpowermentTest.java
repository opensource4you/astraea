package org.astraea.cost;

import java.util.HashMap;
import java.util.Map;
import org.astraea.partitioner.smoothPartitioner.SmoothWeightMetrics;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class EntropyEmpowermentTest {
  @Test
  void test() {
    var smoothWeightMetrics = Mockito.mock(SmoothWeightMetrics.class);
    Map<Integer, Double> input = new HashMap<>();
    input.put(0, 550.0);
    input.put(1, 750.0);
    input.put(2, 550.0);

    Map<Integer, Double> output = new HashMap<>();
    output.put(0, 545.0);
    output.put(1, 755.0);
    output.put(2, 550.0);

    Map<Integer, Double> jvm = new HashMap<>();
    jvm.put(0, 0.254);
    jvm.put(1, 0.268);
    jvm.put(2, 0.235);

    Map<Integer, Double> cpu = new HashMap<>();
    cpu.put(0, 0.1252);
    cpu.put(1, 0.1342);
    cpu.put(2, 0.1522);

    Mockito.when(smoothWeightMetrics.inputCount()).thenReturn(input);
    Mockito.when(smoothWeightMetrics.outputCount()).thenReturn(output);
    Mockito.when(smoothWeightMetrics.jvmUsage()).thenReturn(jvm);
    Mockito.when(smoothWeightMetrics.cpuUsage()).thenReturn(cpu);

    EntropyEmpowerment entropyEmpowerment = new EntropyEmpowerment();
    System.out.println(entropyEmpowerment.entropy(smoothWeightMetrics.inputCount()));
    System.out.println(entropyEmpowerment.entropy(smoothWeightMetrics.outputCount()));
    //    System.out.println(entropyEmpowerment.empowerment(smoothWeightMetrics));
  }
}
