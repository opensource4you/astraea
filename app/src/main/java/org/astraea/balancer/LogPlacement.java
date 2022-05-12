package org.astraea.balancer;

import java.util.Optional;

/** This class describe the placement state of one kafka log. */
public interface LogPlacement {

  int broker();

  Optional<String> logDirectory();

  static LogPlacement of(int broker) {
    return of(broker, null);
  }

  static LogPlacement of(int broker, String logDirectory) {
    return new LogPlacement() {
      @Override
      public int broker() {
        return broker;
      }

      @Override
      public Optional<String> logDirectory() {
        return Optional.ofNullable(logDirectory);
      }

      @Override
      public boolean equals(Object obj) {
        if (obj instanceof LogPlacement) {
          final var that = (LogPlacement) obj;
          return this.broker() == that.broker() && this.logDirectory().equals(that.logDirectory());
        }
        return false;
      }

      @Override
      public String toString() {
        return "LogPlacement{broker=" + broker() + " logDir=" + logDirectory() + "}";
      }
    };
  }
}
