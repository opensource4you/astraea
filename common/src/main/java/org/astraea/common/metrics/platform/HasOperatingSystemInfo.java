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

import org.astraea.common.metrics.HasBeanObject;

public interface HasOperatingSystemInfo extends HasBeanObject {

  /** The operating system architecture. */
  default String arch() {
    return (String) beanObject().attributes().get("Arch");
  }

  /** The number of processors available to the JVM. */
  default int availableProcessors() {
    return (int) beanObject().attributes().get("AvailableProcessors");
  }

  /**
   * The amount of virtual memory that is guaranteed to be available to the running processor, or -1
   * if this operating is not supported.
   */
  default long committedVirtualMemorySize() {
    return (long) beanObject().attributes().get("CommittedVirtualMemorySize");
  }

  /** The amount of free physical memory. */
  default long freePhysicalMemorySize() {
    return (long) beanObject().attributes().get("FreePhysicalMemorySize");
  }

  /** The amount of free swap memory space. */
  default long freeSwapSpaceSize() {
    return (long) beanObject().attributes().get("FreeSwapSpaceSize");
  }

  /** The maximum number of file descriptors. */
  default long maxFileDescriptorCount() {
    return (long) beanObject().attributes().get("MaxFileDescriptorCount");
  }

  /** The number of open file descriptors. */
  default long openFileDescriptorCount() {
    return (long) beanObject().attributes().get("OpenFileDescriptorCount");
  }

  /** The operating system name. */
  default String name() {
    return (String) beanObject().attributes().get("Name");
  }

  /**
   * The amount of CPU load, as a value between 0.0 and 1.0, used by the JVM. When this value is
   * 0.0, the JVM does not use the CPU. If the recent CPU load is not available, the value will be
   * negative.
   */
  default double processCpuLoad() {
    return (double) beanObject().attributes().get("ProcessCpuLoad");
  }

  /**
   * Returns the CPU time used by the process on which the JVM is running in nanoseconds. The
   * returned value is of nanoseconds precision but not necessarily nanoseconds accuracy. This
   * method returns -1 if the platform does not support this operation.
   */
  default long processCpuTime() {
    return (long) beanObject().attributes().get("ProcessCpuTime");
  }

  /**
   * The CPU load of the machine running the JVM in percent of max usage. The machine is running on
   * full load when it reaches 100. If the recent CPU load is not available, the value will be
   * negative.
   */
  default double systemCpuLoad() {
    return (double) beanObject().attributes().get("SystemCpuLoad");
  }

  /** The system load average for the last minute (or a negative value if not available). */
  default double systemLoadAverage() {
    return (double) beanObject().attributes().get("SystemLoadAverage");
  }

  /** The total amount of physical memory */
  default long totalPhysicalMemorySize() {
    return (long) beanObject().attributes().get("TotalPhysicalMemorySize");
  }

  /** The total amount of swap space memory. */
  default long totalSwapSpaceSize() {
    return (long) beanObject().attributes().get("TotalSwapSpaceSize");
  }

  /** The operating system version. */
  default String version() {
    return (String) beanObject().attributes().get("Version");
  }
}
