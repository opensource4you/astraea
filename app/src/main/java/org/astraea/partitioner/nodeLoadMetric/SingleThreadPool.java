package org.astraea.partitioner.nodeLoadMetric;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class offers a simple way to manage the threads. All threads added to builder are not
 * executing right now. Instead, all threads are starting when the pool is built.
 */
public interface SingleThreadPool extends AutoCloseable {

    /** close all running threads. */
    @Override
    void close();

    static Builder builder() {
        return new Builder();
    }

    @FunctionalInterface
    interface Executor {
        /**
         * @throws InterruptedException This is an expected exception if your executor needs to call
         *     blocking method. This exception is not printed to console.
         */
        void execute() throws InterruptedException;

        /** cleanup this executor. */
        default void cleanup() {}
    }

    class Builder {
        private final List<Executor> executors = new ArrayList<>();

        private Builder() {}

        public SingleThreadPool build(Executor ex) {
            Executor executor = ex;
            var closed = new AtomicBoolean(false);
            var latch = new CountDownLatch(executors.size());
            var service = Executors.newSingleThreadExecutor();
                service.execute(
                        () -> {
                            try {
                                var count = 0;
                                while (!closed.get()) {
                                    executor.execute();
                                    count += 1;
                                }
                            } catch (InterruptedException e) {
                                // swallow
                            } finally {
                                try {
                                    executor.cleanup();
                                } finally {
                                    latch.countDown();
                                }
                            }
                        });
            return () -> {
                service.shutdown();
                closed.set(true);
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    // swallow
                }
            };
        }
    }
}
