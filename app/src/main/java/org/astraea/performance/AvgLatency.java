package org.astraea.performance;

public class AvgLatency {
  private double avg;
  private int num;
  private long max;
  private long min;
  private int bytes;

  public AvgLatency() {
    avg = 0;
    num = 0;
    max = 0;
    // 初始為最大的integer值
    min = ~(1 << 31);
    bytes = 0;
  }
  // 多紀錄一個新的值
  public synchronized void put(long latency) {
    if (min > latency) {
      min = latency;
    }
    if (max < latency) {
      max = latency;
    }
    // 記錄現在有幾個數被加入了
    ++num;
    avg += (((double) latency) - avg) / (double) num;
  }
  // 增加bytes數值
  public void addBytes(int bytes) {
    this.bytes += bytes;
  }
  // 取得現在記錄的最大值
  public long getMax() {
    return max;
  }
  // 取得現在記錄的最小值
  public long getMin() {
    return min;
  }
  // 取得現在的平均值
  public double getAvg() {
    return avg;
  }
  // 取得現在輸入/出的byte數，並重置
  public int getBytes() {
    int tmp = this.bytes;
    this.bytes = 0;
    return tmp;
  }
}
