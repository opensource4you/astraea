package org.astraea.metrics.java;

import java.lang.management.MemoryUsage;
import org.astraea.metrics.jmx.BeanObject;

public class JvmMemory implements HasJvmMemory {

  private final BeanObject beanObject;
  private MemoryUsage heapMemoryUsage;
  private MemoryUsage nonHeapMemoryUsage;

  @Override
  public MemoryUsage heapMemoryUsage() {
    // override the default implementation to avoid creating excessive objects
    if (heapMemoryUsage == null) heapMemoryUsage = HasJvmMemory.super.heapMemoryUsage();
    return heapMemoryUsage;
  }

  @Override
  public MemoryUsage nonHeapMemoryUsage() {
    // override the default implementation to avoid creating excessive objects
    if (nonHeapMemoryUsage == null) nonHeapMemoryUsage = HasJvmMemory.super.nonHeapMemoryUsage();
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
            .append("    HeapMemoryUsage: ")
            .append(heapMemoryUsage())
            .append(System.lineSeparator())
            .append("    NonHeapMemoryUsage")
            .append(nonHeapMemoryUsage());
    return "JvmMemory {\n" + sb + "\n}";
  }
}
