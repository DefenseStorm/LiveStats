package io.praesid.livestats;

import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class ServiceStats {
    private static final Logger log = LogManager.getLogger();

    private Map<String, LiveStats> stats = new ConcurrentSkipListMap<>();
    private final Function<String, LiveStats> statsMaker;

    public ServiceStats(final double... quantiles) {
        statsMaker = ignored -> new LiveStats(quantiles);
    }

    public ServiceStats(final double decayMultiplier, final Duration decayPeriod, final double... quantiles) {
        statsMaker = ignored -> new LiveStats(decayMultiplier, decayPeriod, quantiles);
    }

    public void put(final String key, final double value) {
        addTiming(key, value, System.nanoTime());
    }

    public void complete(final String key, final long startNanos) {
        final long endNanos = System.nanoTime();
        addTiming(key, endNanos - startNanos, endNanos);
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
    public Stats[] consume() {
        final List<Entry<String, LiveStats>> savedStats = new ArrayList<>();
        final Iterator<Entry<String, LiveStats>> entryIterator = stats.entrySet().iterator();
        while (entryIterator.hasNext()) {
            savedStats.add(entryIterator.next());
            entryIterator.remove();
        }
        savedStats.forEach(e -> e.getValue().decay());
        // Stats overhead is in the microsecond range, so give a millisecond here for anyone in addTiming() to finish
        Uninterruptibles.sleepUninterruptibly(1, TimeUnit.MILLISECONDS);
        return savedStats.stream().map(e -> new Stats(e.getKey(), e.getValue())).toArray(Stats[]::new);
    }

    public Stream<Stats> get() {
        return stats.entrySet().stream().peek(e -> e.getValue().decay()).map(e -> new Stats(e.getKey(), e.getValue()));
    }

    private void addTiming(final String key, final double value, final long endNanos) {
        stats.computeIfAbsent(key, statsMaker).add(value);
        if (log.isTraceEnabled()) {
            final long overhead = System.nanoTime() - endNanos;
            stats.computeIfAbsent("overhead", statsMaker).add(overhead);
        }
    }

    private String appendSubType(final String description, final boolean success, final boolean error) {
        // Check error first because collectTiming(description, subject) always passes true for success
        final String subType = error ? "error" : success ? "success" : "failure";
        return description + '/' + subType;
    }
}
