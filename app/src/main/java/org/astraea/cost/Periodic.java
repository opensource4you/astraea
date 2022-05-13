package org.astraea.cost;

import java.util.function.Supplier;
import org.astraea.Utils;

/** Enables methods to be updated periodically, not calculated every time they are called. */
public abstract class Periodic<Value> {
  private long lastUpdate = -1;
  private Value value;

  /**
   * Updates the value each second.
   *
   * @param updater Methods that need to be updated regularly.
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

  /**
   * Updates the value interval second.
   *
   * @param updater Methods that need to be updated regularly.
   * @param interval Required interval.
   * @return an object of type Value created from the parameter value.
   */
  protected Value tryUpdate(Supplier<Value> updater, int interval) {
    if (Utils.overSecond(lastUpdate, interval)) {
      value = updater.get();
      lastUpdate = currentTime();
    }
    return value;
  }
}
