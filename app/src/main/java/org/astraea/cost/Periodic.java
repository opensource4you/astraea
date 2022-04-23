package org.astraea.cost;

import java.util.function.Supplier;
import org.astraea.Utils;

public abstract class Periodic<Value> {
  private long lastUpdate = -1;
  private Value value;
  /**
   * Updates the value each second.
   *
   * @return an object of type Value created from the parameter value.
   */
  protected Value tryUpdate(Supplier<Value> updater) {
    if (Utils.overSecond(lastUpdate, 1)) {
      value = updater.get();
      lastUpdate = currentTime();
    }
    return value;
  }

  protected long currentTime() {
    return System.currentTimeMillis();
  }
}
