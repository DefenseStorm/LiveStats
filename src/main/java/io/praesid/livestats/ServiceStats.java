package io.praesid.livestats;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.concurrent.GuardedBy;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class ServiceStats {
    private static final Logger log = LogManager.getLogger();

    private static final ServiceStats NOOP = new NoopServiceStats();

    private static final StampedLock lock = new StampedLock();
    @GuardedBy("lock")
    private static ServiceStats instance = null;

    private ServiceStats() {}

    /**
     * Puts a timing entry of `nanos` into the stats for `key`.
     * @param key The key to add a timing entry for.
     * @param nanos The number of nanoseconds to record.
     */
    public void put(final String key, final long nanos) {
        addTiming(key, nanos, System.nanoTime());
    }

    /**
     * Puts a timing entry of `System.nanoTime() - startNanos` into the stats for `key`.
     * @param key The key to add a timing entry for.
     * @param startNanos The System.nanoTime() at which the subject task started.
     */
    public void complete(final String key, final long startNanos) {
        final long endNanos = System.nanoTime();
        addTiming(key, endNanos - startNanos, endNanos);
    }

    /**
     * Records the execution time for the future produced by `subject` at a key based on `name`.
     *
     * The key for the stats is "`name`/success" unless the task throws an exception, then it's "`name`/error".
     *
     * @param name The base name used to determine which stats key to store the resulting timing entry.
     * @param subject A supplier which starts the task to be timed.
     * @param <T>
     * @return The same future produced by `subject`.
     */
    public <T> CompletableFuture<T> timingOnCompletion(
            final String name, final Supplier<CompletableFuture<T>> subject) {
        final long startNanos = System.nanoTime();
        final CompletableFuture<T> future = subject.get();
        future.whenComplete((result, thrown) -> {
            if (thrown == null) {
                complete(appendSubType(name, true, false), startNanos);
            } else {
                complete(appendSubType(name, false, true), startNanos);
            }
        });
        return future;
    }

    /**
     * Records the execution time for the future produced by `subject` at a key based on `name`.
     *
     * The key for the stats is "`name`/success" or "`name`/failure" based on the supplied predicate,
     * unless the task throws an exception, then it's "`name`/error".
     *
     * @param name The base name used to determine which stats key to store the resulting timing entry.
     * @param subject A supplier which starts the code to be timed.
     * @param successful A predicate on the result of the supplied future to determine whether the task was successful.
     * @param <T>
     * @return The same future produced by `subject`.
     */
    public <T> CompletableFuture<T> timingOnCompletion(
            final String name, final Supplier<CompletableFuture<T>> subject, final Predicate<T> successful) {
        final long start = System.nanoTime();
        final CompletableFuture<T> future = subject.get();
        future.whenComplete((result, thrown) -> {
            if (thrown == null) {
                final long end = System.nanoTime(); // Count success test in overhead
                addTiming(appendSubType(name, successful.test(result), false), end - start, end);
            } else {
                complete(appendSubType(name, false, true), start);
            }
        });
        return future;
    }

    /**
     * Records the execution time for `subject` at a key based on `name`.
     *
     * The key for the stats is "`name`/success" unless the task throws an exception, then it's "`name`/error".
     *
     * @param name The base name used to determine which stats key to store the resulting timing entry.
     * @param subject The task to be timed.
     * @param <T>
     * @return The same result produced by `subject`.
     */
    public <T> T collectTiming(final String name, final Supplier<T> subject) {
        final long startNanos = System.nanoTime();
        boolean error = false;
        try {
            return subject.get();
        } catch (final Throwable t) {
            error = true;
            throw t;
        } finally {
            complete(appendSubType(name, true, error), startNanos);
        }
    }

    /**
     * Records the execution time for `subject` at a key based on `name`.
     *
     * The key for the stats is "`name`/success" unless the task throws, then it's "`name`/error" or "`name`/failure"
     * based on the supplied function.
     *
     * @param name The base name used to determine which stats key to store the resulting timing entry.
     * @param subject The task to be timed.
     * @param expected A predicate to indicate whether an thrown Throwable is an expected 'failure' or an 'error'.
     * @param <T>
     * @return The same result produced by `subject`.
     */
    public <T> T collectTimingWithThrownFailures(final String name, final Supplier<T> subject,
                                                 final Predicate<Throwable> expected) {
        final long start = System.nanoTime();
        boolean success = false;
        boolean error = false;
        long end = -1;
        try {
            final T result = subject.get();
            end = System.nanoTime(); // because of counting the expected check as overhead on throw
            success = true;
            return result;
        } catch (final Throwable t) {
            end = System.nanoTime(); // count expected check as overhead
            error = !expected.test(t);
            throw t;
        } finally {
            addTiming(appendSubType(name, success, error), end - start, end);
        }
    }

    /**
     * Records the execution time for `subject` at a key based on `name`.
     *
     * The key for the stats is "`name`/success" or "`name`/failure" based on the supplied predicate,
     * unless the task throws an exception, then it's "`name`/error".
     *
     * @param name The base name used to determine which stats key to store the resulting timing entry.
     * @param subject The task to be timed.
     * @param successful A predicate on the result of the supplied future to determine whether the task was successful.
     * @param <T>
     * @return The same result produced by `subject`.
     */
    public <T> T collectTiming(final String name, final Supplier<T> subject, final Predicate<T> successful) {
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
            addTiming(appendSubType(name, success, error), end - start, end);
        }
    }

    /**
     * Consumes a snapshot of the live stats currently collected.
     * Usually, this method makes sense when using non-decaying stats.
     * NOTE: This is a destructive call, the returned stats are the only remaining reference to the collected data.
     * @return stats snapshots
     */
    public abstract Stats[] consume();
    /**
     * Gets a snapshot of the live stats currently collected.
     * Usually, this method makes sense when using decaying stats.
     * @return stats snapshots
     */
    public abstract Stream<Stats> get(final String... statsNames);
    protected abstract void addTiming(final String key, final long nanos, final long endNanos);

    private static String appendSubType(final String name, final boolean success, final boolean error) {
        // Check error first because collectTiming(name, subject) always passes true for success
        final String subType = error ? "error" : success ? "success" : "failure";
        return name + '/' + subType;
    }

    public static void disable() {
        final long stamp = lock.writeLock();
        instance = null;
        lock.unlock(stamp);
    }

    public static void configure(final double... quantiles) {
        configure(DecayConfig.NEVER, quantiles);
    }

    public static void configure(final DecayConfig decayConfig, final double... quantiles) {
        configure(decayConfig, Collections.emptyMap(), quantiles);
    }

    public static void configure(final DecayConfig defaultDecayConfig, final Map<String, DecayConfig> decayConfigMap,
                                 final double... quantiles) {
        final long stamp = lock.writeLock();
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
        @Override public Stream<Stats> get(final String... statsNames) { return Stream.of(); }
        @Override protected void addTiming(final String key, final long nanos, final long endNanos) { }
    }

    private static class RealServiceStats extends ServiceStats {
        private final ConcurrentMap<String, LiveStats> stats = new ConcurrentHashMap<>();
        private final Function<String, DecayConfig> getDecayConfig;
        private final double[] quantiles;

        private RealServiceStats(final DecayConfig defaultDecayConfig, final Map<String, DecayConfig> decayConfigMap,
                                 final double... quantiles) {
            this.getDecayConfig = key -> decayConfigMap.getOrDefault(key, defaultDecayConfig);
            this.quantiles = Arrays.copyOf(quantiles, quantiles.length);
        }

        @Override
        public Stats[] consume() {
            final Map<String, LiveStats> savedStats = new TreeMap<>(stats);
            savedStats.keySet().forEach(stats::remove);
            // Stats overhead is in the microsecond range, give a millisecond here for anyone in addTiming() to finish
            try {
                Thread.sleep(1);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return savedStats.entrySet().stream()
                             .peek(e -> e.getValue().decayByTime())
                             .map(e -> new Stats(e.getKey(), e.getValue()))
                             .toArray(Stats[]::new);
        }

        @Override
        public Stream<Stats> get(final String... statsNames) {
            final Map<String, LiveStats> statsToReturn =
                    statsNames.length == 0 ? Collections.unmodifiableMap(stats) :
                            Arrays.stream(statsNames)
                                  .collect(Collectors.toMap(Function.identity(), stats::get));
            return statsToReturn.entrySet().stream()
                       .peek(e -> e.getValue().decayByTime())
                       .map(e -> new Stats(e.getKey(), e.getValue()));
        }

        @Override
        protected void addTiming(final String key, final long nanos, final long endNanos) {
            stats.computeIfAbsent(key, name -> new LiveStats(getDecayConfig.apply(name), quantiles)).add(nanos);
            if (log.isTraceEnabled()) {
                final long overhead = System.nanoTime() - endNanos;
                stats.computeIfAbsent("overhead", name -> new LiveStats(getDecayConfig.apply(name), quantiles))
                     .add(overhead);
            }
        }
    }
}
