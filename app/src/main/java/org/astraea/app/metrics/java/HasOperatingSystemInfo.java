package org.astraea.app.metrics.java;

import org.astraea.app.metrics.HasBeanObject;

public interface HasOperatingSystemInfo extends HasBeanObject {

  /** The operating system architecture. */
  default String arch() {
    return (String) beanObject().getAttributes().get("Arch");
  }

  /** The number of processors available to the JVM. */
  default int availableProcessors() {
    return (int) beanObject().getAttributes().get("AvailableProcessors");
  }

  /**
   * The amount of virtual memory that is guaranteed to be available to the running processor, or -1
   * if this operating is not supported.
   */
  default long committedVirtualMemorySize() {
    return (long) beanObject().getAttributes().get("CommittedVirtualMemorySize");
  }

  /** The amount of free physical memory. */
  default long freePhysicalMemorySize() {
    return (long) beanObject().getAttributes().get("FreePhysicalMemorySize");
  }

  /** The amount of free swap memory space. */
  default long freeSwapSpaceSize() {
    return (long) beanObject().getAttributes().get("FreeSwapSpaceSize");
  }

  /** The maximum number of file descriptors. */
  default long maxFileDescriptorCount() {
    return (long) beanObject().getAttributes().get("MaxFileDescriptorCount");
  }

  /** The number of open file descriptors. */
  default long openFileDescriptorCount() {
    return (long) beanObject().getAttributes().get("OpenFileDescriptorCount");
  }

  /** The operating system name. */
  default String name() {
    return (String) beanObject().getAttributes().get("Name");
  }

  /**
   * The amount of CPU load, as a value between 0.0 and 1.0, used by the JVM. When this value is
   * 0.0, the JVM does not use the CPU. If the recent CPU load is not available, the value will be
   * negative.
   */
  default double processCpuLoad() {
    return (double) beanObject().getAttributes().get("ProcessCpuLoad");
  }

  /**
   * Returns the CPU time used by the process on which the JVM is running in nanoseconds. The
   * returned value is of nanoseconds precision but not necessarily nanoseconds accuracy. This
   * method returns -1 if the platform does not support this operation.
   */
  default long processCpuTime() {
    return (long) beanObject().getAttributes().get("ProcessCpuTime");
  }

  /**
   * The CPU load of the machine running the JVM in percent of max usage. The machine is running on
   * full load when it reaches 100. If the recent CPU load is not available, the value will be
   * negative.
   */
  default double systemCpuLoad() {
    return (double) beanObject().getAttributes().get("SystemCpuLoad");
  }

  /** The system load average for the last minute (or a negative value if not available). */
  default double systemLoadAverage() {
    return (double) beanObject().getAttributes().get("SystemLoadAverage");
  }

  /** The total amount of physical memory */
  default long totalPhysicalMemorySize() {
    return (long) beanObject().getAttributes().get("TotalPhysicalMemorySize");
  }

  /** The total amount of swap space memory. */
  default long totalSwapSpaceSize() {
    return (long) beanObject().getAttributes().get("TotalSwapSpaceSize");
  }

  /** The operating system version. */
  default String version() {
    return (String) beanObject().getAttributes().get("Version");
  }
}
