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
}
