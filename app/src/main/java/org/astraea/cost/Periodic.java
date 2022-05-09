package org.astraea.cost;

import java.time.Duration;
import java.util.function.Supplier;
import org.astraea.Utils;

/** Enables methods to be updated periodically, not calculated every time they are called. */
public abstract class Periodic<Value> {
  private long lastUpdate = -1;
  private Value value;

  /**
   * Updates the value interval second.
   *
   * @param updater Methods that need to be updated regularly.
   * @param interval Required interval.
   * @return an object of type Value created from the parameter value.
   */
  protected Value tryUpdate(Supplier<Value> updater, Duration interval) {
    if (Utils.isExpired(lastUpdate, interval.toSecondsPart())) {
      value = updater.get();
      lastUpdate = currentTime();
    }
    return value;
  }

  /**
   * Updates the value each second.
   *
   * @param updater Methods that need to be updated regularly.
   * @return an object of type Value created from the parameter value.
   */
  protected Value tryUpdateAfterOneSecond(Supplier<Value> updater) {
    if (Utils.isExpired(lastUpdate, 1)) {
      value = updater.get();
      lastUpdate = currentTime();
    }
    return value;
  }

  protected long currentTime() {
    return System.currentTimeMillis();
  }
}
