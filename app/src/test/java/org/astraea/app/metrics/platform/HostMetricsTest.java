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
package org.astraea.app.metrics.platform;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.condition.OS.LINUX;
import static org.junit.jupiter.api.condition.OS.WINDOWS;

import org.astraea.app.metrics.MBeanClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.EnabledOnOs;

public class HostMetricsTest {

  @Test
  void operatingSystem() {
    var operatingSystemInfo = HostMetrics.operatingSystem(MBeanClient.local());
    assertDoesNotThrow(operatingSystemInfo::arch);
    assertDoesNotThrow(operatingSystemInfo::availableProcessors);
    assertDoesNotThrow(operatingSystemInfo::committedVirtualMemorySize);
    assertDoesNotThrow(operatingSystemInfo::freePhysicalMemorySize);
    assertDoesNotThrow(operatingSystemInfo::freeSwapSpaceSize);
    assertDoesNotThrow(operatingSystemInfo::name);
    assertDoesNotThrow(operatingSystemInfo::processCpuLoad);
    assertDoesNotThrow(operatingSystemInfo::processCpuTime);
    assertDoesNotThrow(operatingSystemInfo::systemCpuLoad);
    assertDoesNotThrow(operatingSystemInfo::systemLoadAverage);
    assertDoesNotThrow(operatingSystemInfo::totalPhysicalMemorySize);
    assertDoesNotThrow(operatingSystemInfo::totalSwapSpaceSize);
    assertDoesNotThrow(operatingSystemInfo::version);
  }

  @Test
  @DisabledOnOs(WINDOWS)
  void maxFileDescriptorCount() {
    OperatingSystemInfo operatingSystemInfo = HostMetrics.operatingSystem(MBeanClient.local());
    assertDoesNotThrow(operatingSystemInfo::maxFileDescriptorCount);
  }

  @Test
  @DisabledOnOs(WINDOWS)
  void openFileDescriptorCount() {
    OperatingSystemInfo operatingSystemInfo = HostMetrics.operatingSystem(MBeanClient.local());
    assertDoesNotThrow(operatingSystemInfo::openFileDescriptorCount);
  }

  @Test
  void testJvmMemory() {
    JvmMemory jvmMemory = HostMetrics.jvmMemory(MBeanClient.local());
    assertDoesNotThrow(() -> jvmMemory.heapMemoryUsage().getCommitted());
    assertDoesNotThrow(() -> jvmMemory.heapMemoryUsage().getMax());
    assertDoesNotThrow(() -> jvmMemory.heapMemoryUsage().getUsed());
    assertDoesNotThrow(() -> jvmMemory.heapMemoryUsage().getInit());
    assertDoesNotThrow(() -> jvmMemory.nonHeapMemoryUsage().getCommitted());
    assertDoesNotThrow(() -> jvmMemory.nonHeapMemoryUsage().getMax());
    assertDoesNotThrow(() -> jvmMemory.nonHeapMemoryUsage().getUsed());
    assertDoesNotThrow(() -> jvmMemory.nonHeapMemoryUsage().getInit());
  }

  @Test
  @EnabledOnOs(LINUX)
  void linuxDiskReadBytes() {
    assertDoesNotThrow(() -> HostMetrics.linuxDiskReadBytes(MBeanClient.local()));
  }

  @Test
  @EnabledOnOs(LINUX)
  void linuxDiskWriteBytes() {
    assertDoesNotThrow(() -> HostMetrics.linuxDiskWriteBytes(MBeanClient.local()));
  }
}
