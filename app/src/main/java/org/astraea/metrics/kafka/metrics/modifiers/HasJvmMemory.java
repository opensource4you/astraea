package org.astraea.metrics.kafka.metrics.modifiers;

import java.lang.management.MemoryUsage;

public interface HasJvmMemory extends HasBeanObject {

  MemoryUsage heapMemoryUsage();

  MemoryUsage nonHeapMemoryUsage();
}
