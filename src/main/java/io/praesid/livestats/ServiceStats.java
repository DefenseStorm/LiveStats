package io.praesid.livestats;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.concurrent.GuardedBy;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

public abstract class ServiceStats {
    private static final Logger log = LogManager.getLogger();

    private static final ServiceStats NOOP = new NoopServiceStats();

    private static final StampedLock lock = new StampedLock();
    @GuardedBy("lock")
    private static ServiceStats instance = null;

    private ServiceStats() {}

    public void put(final String key, final double value) {
        addTiming(key, value, System.nanoTime());
    }

    public void complete(final String key, final long startNanos) {
        final long endNanos = System.nanoTime();
        addTiming(key, endNanos - startNanos, endNanos);
    }

    public <T> ListenableFuture<T> listenForTiming(
            final String description, final Supplier<ListenableFuture<T>> subject) {
        final long startNanos = System.nanoTime();
        final ListenableFuture<T> future = subject.get();
        future.addListener(() -> {
            try {
                future.get();
                complete(appendSubType(description, true, false), startNanos);
            } catch (final Throwable t) {
                complete(appendSubType(description, false, true), startNanos);
            }
        }, MoreExecutors.directExecutor());
        return future;
    }

    public <T> ListenableFuture<T> listenForTiming(
            final String description, final Supplier<ListenableFuture<T>> subject, final Predicate<T> successful) {
        final long start = System.nanoTime();
        final ListenableFuture<T> future = subject.get();
        future.addListener(() -> {
            try {
                final T result = future.get();
                final long end = System.nanoTime(); // Count success test in overhead
                addTiming(appendSubType(description, successful.test(result), false), end - start, end);
            } catch (final Throwable t) {
                complete(appendSubType(description, false, true), start);
            }
        }, MoreExecutors.directExecutor());
        return future;
    }

    public <T> T collectTiming(final String description, final Supplier<T> subject) {
        final long startNanos = System.nanoTime();
        boolean error = false;
        try {
            return subject.get();
        } catch (final Throwable t) {
            error = true;
            throw t;
        } finally {
            complete(appendSubType(description, true, error), startNanos);
        }
    }

    public <T> T collectTiming(final String description, final Supplier<T> subject, final Predicate<T> successful) {
        final long start = System.nanoTime();
        boolean success = false;
        boolean error = false;
        long end = -1;
        try {
            final T result = subject.get();
            end = System.nanoTime(); // count predicate check as overhead
            success = successful.test(result);
            return result;
        } catch (final Throwable t) {
            end = System.nanoTime(); // because of counting the predicate as overhead on success
            error = true;
            throw t;
        } finally {
            addTiming(appendSubType(description, success, error), end - start, end);
        }
    }

    /**
     * Consumes a snapshot of the live stats currently collected
     * @return stats snapshots
     */
    public abstract Stats[] consume();
    public abstract Stream<Stats> get();
    protected abstract void addTiming(final String key, final double value, final long endNanos);

    private static String appendSubType(final String description, final boolean success, final boolean error) {
        // Check error first because collectTiming(description, subject) always passes true for success
        final String subType = error ? "error" : success ? "success" : "failure";
        return description + '/' + subType;
    }

    public static void configure(final double... quantiles) {
        configure(DecayConfig.NEVER, quantiles);
    }

    public static void configure(final DecayConfig decayConfig, final double... quantiles) {
        configure(decayConfig, ImmutableMap.of(), quantiles);
    }

    public static void configure(final DecayConfig defaultDecayConfig, final Map<String, DecayConfig> decayConfigMap,
                                 final double... quantiles) {
        final long stamp = lock.writeLock();
        //noinspection VariableNotUsedInsideIf
        if (instance != null) {
            throw new IllegalStateException("ServiceStats does not support reconfiguration");
        }
        instance = new RealServiceStats(defaultDecayConfig, decayConfigMap, quantiles);
        lock.unlock(stamp);
    }

    public static ServiceStats instance() {
        final long optimisticStamp = lock.tryOptimisticRead();
        ServiceStats myInstance = instance;
        if (!lock.validate(optimisticStamp)) {
            final long readStamp = lock.readLock();
            myInstance = instance;
            lock.unlock(readStamp);
        }
        return myInstance == null ? NOOP : myInstance;
    }


    private static class NoopServiceStats extends ServiceStats {
        @Override public Stats[] consume() { return new Stats[0]; }
        @Override public Stream<Stats> get() { return Stream.of(); }
        @Override protected void addTiming(final String key, final double value, final long endNanos) { }
    }

    private static class RealServiceStats extends ServiceStats {
        private Map<String, LiveStats> stats = new ConcurrentSkipListMap<>();
        private final Function<String, LiveStats> statsMaker;

        private RealServiceStats(final DecayConfig defaultDecayConfig, final Map<String, DecayConfig> decayConfigMap,
                                 final double... quantiles) {
            statsMaker = key -> new LiveStats(decayConfigMap.getOrDefault(key, defaultDecayConfig), quantiles);
        }

        @Override
        public Stats[] consume() {
            final List<Entry<String, LiveStats>> savedStats = new ArrayList<>();
            final Iterator<Entry<String, LiveStats>> entryIterator = stats.entrySet().iterator();
            while (entryIterator.hasNext()) {
                savedStats.add(entryIterator.next());
                entryIterator.remove();
            }
            savedStats.forEach(e -> e.getValue().decay());
            // Stats overhead is in the microsecond range, give a millisecond here for anyone in addTiming() to finish
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.MILLISECONDS);
            return savedStats.stream().map(e -> new Stats(e.getKey(), e.getValue())).toArray(Stats[]::new);
        }

        @Override
        public Stream<Stats> get() {
            return stats.entrySet().stream()
                        .peek(e -> e.getValue().decay())
                        .map(e -> new Stats(e.getKey(), e.getValue()));
        }

        @Override
        protected void addTiming(final String key, final double value, final long endNanos) {
            stats.computeIfAbsent(key, statsMaker).add(value);
            if (log.isTraceEnabled()) {
                final long overhead = System.nanoTime() - endNanos;
                stats.computeIfAbsent("overhead", statsMaker).add(overhead);
            }
        }
    }
}
