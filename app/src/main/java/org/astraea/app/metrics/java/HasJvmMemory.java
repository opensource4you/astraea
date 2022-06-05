package org.astraea.app.metrics.java;

import java.lang.management.MemoryUsage;
import javax.management.openmbean.CompositeData;
import org.astraea.app.metrics.HasBeanObject;

public interface HasJvmMemory extends HasBeanObject {

  default MemoryUsage heapMemoryUsage() {
    return MemoryUsage.from((CompositeData) beanObject().getAttributes().get("HeapMemoryUsage"));
  }

  default MemoryUsage nonHeapMemoryUsage() {
    return MemoryUsage.from((CompositeData) beanObject().getAttributes().get("NonHeapMemoryUsage"));
  }
}
