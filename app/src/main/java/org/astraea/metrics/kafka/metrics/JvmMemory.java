package org.astraea.metrics.kafka.metrics;

import java.lang.management.MemoryUsage;
import javax.management.openmbean.CompositeData;
import org.astraea.metrics.jmx.BeanObject;
import org.astraea.metrics.kafka.metrics.modifiers.HasJvmMemory;

public class JvmMemory implements HasJvmMemory {

  private final BeanObject beanObject;
  private MemoryUsage heapMemoryUsage;
  private MemoryUsage nonHeapMemoryUsage;

  @Override
  public MemoryUsage heapMemoryUsage() {
    if (heapMemoryUsage == null)
      heapMemoryUsage =
          MemoryUsage.from((CompositeData) beanObject.getAttributes().get("HeapMemoryUsage"));
    return heapMemoryUsage;
  }

  @Override
  public MemoryUsage nonHeapMemoryUsage() {
    if (nonHeapMemoryUsage == null)
      nonHeapMemoryUsage =
          MemoryUsage.from((CompositeData) beanObject.getAttributes().get("NonHeapMemoryUsage"));
    return nonHeapMemoryUsage;
  }

  public JvmMemory(BeanObject beanObject) {
    this.beanObject = beanObject;
  }

  @Override
  public BeanObject beanObject() {
    return beanObject;
  }

  @Override
  public String toString() {
    StringBuilder sb =
        new StringBuilder()
            .append(System.lineSeparator())
            .append("  ")
            .append("HeapMemoryUsage Max: ")
            .append(heapMemoryUsage().getMax())
            .append(System.lineSeparator())
            .append("  ")
            .append("HeapMemoryUsage Committed: ")
            .append(heapMemoryUsage().getCommitted())
            .append(System.lineSeparator())
            .append("  ")
            .append("HeapMemoryUsage Used: ")
            .append(heapMemoryUsage().getUsed())
            .append(System.lineSeparator())
            .append("  ")
            .append("HeapMemoryUsage Init: ")
            .append(heapMemoryUsage().getInit())
            .append(System.lineSeparator())
            .append("  ")
            .append("NonHeapMemoryUsage Max: ")
            .append(nonHeapMemoryUsage().getMax())
            .append(System.lineSeparator())
            .append("  ")
            .append("NonHeapMemoryUsage Committed: ")
            .append(nonHeapMemoryUsage().getCommitted())
            .append(System.lineSeparator())
            .append("  ")
            .append("NonHeapMemoryUsage Used: ")
            .append(nonHeapMemoryUsage().getUsed())
            .append(System.lineSeparator())
            .append("  ")
            .append("NonHeapMemoryUsage Init: ")
            .append(nonHeapMemoryUsage().getInit());
    return "JvmMemory {" + sb + "}";
  }
}
