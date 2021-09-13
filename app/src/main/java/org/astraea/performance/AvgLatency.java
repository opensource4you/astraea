package org.astraea.performance;

import java.util.concurrent.atomic.LongAdder;

public class AvgLatency {
  private double avg;
  private long num;
  private long max;
  private long min;
  private LongAdder bytes;

  public AvgLatency() {
    avg = 0;
    num = 0;
    max = 0;
    // 初始為最大的integer值
    min = Long.MAX_VALUE;
    bytes = new LongAdder();
  }
  // 多紀錄一個新的值
  public void put(long latency) {
    // 更新最大延時
    if (min > latency) {
      min = latency;
    }
    // 更新最小延時
    if (max < latency) {
      max = latency;
    }
    // 記錄現在有幾個數被加入了
    ++num;
    // 更新平均值
    avg += (((double) latency) - avg) / (double) num;
  }
  // 增加bytes數值
  public void addBytes(long bytes) {
    this.bytes.add(bytes);
  }

  public long num() {
    return num;
  }
  // 取得現在記錄的最大值
  public long max() {
    return max;
  }
  // 取得現在記錄的最小值
  public long min() {
    return min;
  }
  // 取得現在的平均值
  public double avg() {
    return avg;
  }
  // 取得從 上次呼叫"getBytes()" 到 現在 的輸入/出的byte數，並重置
  public long bytes() {
    return this.bytes.sumThenReset();
  }
}
