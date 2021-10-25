package org.astraea.metrics.kafka.metrics.modifiers;

import java.lang.management.MemoryUsage;
import javax.management.openmbean.CompositeData;

public interface HasJvmMemory extends HasBeanObject {

  default MemoryUsage heapMemoryUsage() {
    return MemoryUsage.from((CompositeData) beanObject().getAttributes().get("HeapMemoryUsage"));
  }

  default MemoryUsage nonHeapMemoryUsage() {
    return MemoryUsage.from((CompositeData) beanObject().getAttributes().get("NonHeapMemoryUsage"));
  }
}
