package org.astraea.performance;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.astraea.common.DataSize;
import org.astraea.common.DataUnit;

@FunctionalInterface
interface DataSupplier extends Supplier<DataSupplier.Data> {

  static Data data(byte[] key, byte[] value) {
    return new Data() {
      @Override
      public boolean done() {
        return false;
      }

      @Override
      public boolean throttled() {
        return false;
      }

      @Override
      public byte[] key() {
        return key;
      }

      @Override
      public byte[] value() {
        return value;
      }
    };
  }

  Data NO_MORE_DATA =
      new Data() {
        @Override
        public boolean done() {
          return true;
        }

        @Override
        public boolean throttled() {
          return false;
        }

        @Override
        public byte[] key() {
          throw new IllegalStateException("there is no data");
        }

        @Override
        public byte[] value() {
          throw new IllegalStateException("there is no data");
        }
      };

  Data THROTTLED_DATA =
      new Data() {
        @Override
        public boolean done() {
          return false;
        }

        @Override
        public boolean throttled() {
          return true;
        }

        @Override
        public byte[] key() {
          throw new IllegalStateException("it is throttled");
        }

        @Override
        public byte[] value() {
          throw new IllegalStateException("it is throttled");
        }
      };

  interface Data {

    /** @return true if there is no data. */
    boolean done();

    /** @return true if there are some data, but it is throttled now. */
    boolean throttled();

    /** @return true if there is accessible data */
    default boolean hasData() {
      return !done() && !throttled();
    }

    /** @return key or throw exception if there is no data, or it is throttled now */
    byte[] key();

    /** @return value or throw exception if there is no data, or it is throttled now */
    byte[] value();
  }

  static DataSupplier of(
      ExeTime exeTime,
      Supplier<Long> keyDistribution,
      DataSize valueSize,
      Supplier<Long> valueDistribution,
      DataSize throughput) {
    return new DataSupplier() {
      private final long start = System.currentTimeMillis();
      private final Random rand = new Random();
      private final byte[] content = new byte[valueSize.measurement(DataUnit.Byte).intValue()];
      private final AtomicLong dataCount = new AtomicLong(0);
      private long intervalStart = 0;
      private long payloadBytes;

      synchronized boolean checkAndAdd(int payloadLength) {
        if (System.currentTimeMillis() - intervalStart > 1000) {
          intervalStart = System.currentTimeMillis();
          payloadBytes = payloadLength;
          return true;
        } else if (payloadBytes < throughput.measurement(DataUnit.Byte).longValue()) {
          payloadBytes += payloadLength;
          return true;
        } else {
          return false;
        }
      }

      byte[] value() {
        // Randomly change one position of the content;
        content[rand.nextInt(content.length)] = (byte) rand.nextInt(256);
        return Arrays.copyOfRange(
            content, (int) (valueDistribution.get() % content.length), content.length);
      }

      public byte[] key() {
        return (String.valueOf(keyDistribution.get())).getBytes();
      }

      @Override
      public Data get() {
        if (exeTime.percentage(dataCount.getAndIncrement(), System.currentTimeMillis() - start)
            >= 100D) return NO_MORE_DATA;
        var key = key();
        var value = value();
        if (checkAndAdd(value.length)) return data(key, value);
        return THROTTLED_DATA;
      }
    };
  }
}
