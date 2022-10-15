/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.astraea.common.metrics.platform;

import java.lang.management.MemoryUsage;
import org.astraea.common.metrics.BeanObject;

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
