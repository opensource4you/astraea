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
   * @param second Required interval.
   * @return an object of type Value created from the parameter value.
   */
  protected Value tryUpdate(Supplier<Value> updater, int second) {
    if (Utils.overSecond(lastUpdate, second)) {
      value = updater.get();
      lastUpdate = currentTime();
    }
    return value;
  }

  protected long currentTime() {
    return System.currentTimeMillis();
  }
}
