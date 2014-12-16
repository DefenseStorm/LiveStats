package io.praesid.livestats;

import com.google.common.collect.ImmutableMap;
import lombok.ToString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class ServiceStats implements BiConsumer<String, Double> {
    private static final Logger log = LogManager.getLogger();

    private Map<String, LiveStats> stats = new ConcurrentSkipListMap<>();
    private final Function<String, LiveStats> statsMaker;

    public ServiceStats(final double sampleProbability, final double... quantiles) {
        statsMaker = ignored -> new LiveStats(sampleProbability, quantiles);
    }

    public void addTiming(final String key, final double value) {
        addTiming(key, value, System.nanoTime());
    }

    @Override
    public void accept(final String key, final Double value) {
        addTiming(key, value);
    }

    private void addTiming(final String key, final double value, final long end) {
        final boolean fullStats = stats.computeIfAbsent(key, statsMaker).add(value);
        final long overhead = System.nanoTime() - end;
        if (log.isTraceEnabled()) {
            stats.computeIfAbsent("overhead/" + (fullStats ? "full" : "short"), statsMaker).add(overhead);
        }
    }

    public <T> T collectTiming(final String description, final Supplier<T> subjectFn, final Predicate<T> successful) {
        final long start = System.nanoTime();
        boolean success = false;
        Throwable ex = null;
        long end = -1;
        try {
            final T result = subjectFn.get();
            end = System.nanoTime(); // count predicate check as overhead
            success = successful.test(result);
            return result;
        } catch (final Throwable t) {
            end = System.nanoTime(); // because of counting the predicate as overhead on success
            ex = t;
            throw t;
        } finally {
            final long nanos = end - start;
            final String key = description + '/' + (success ? "success" : ex == null ? "failure" : "exception");
            addTiming(key, nanos, end);
        }
    }

    public void clear() {
        stats.clear();
    }

    public Stream<Summary> sumaries() {
        return stats.entrySet().stream().map(e -> new Summary(e.getKey(), e.getValue()));
    }

    @ToString
    public static final class Summary {
        public final String name;
        public final int n;
        public final int min;
        public final int max;
        public final double mean;
        public final double skewness;
        public final double kurtosis;
        public final Map<Double, Double> quantiles;

        public Summary(final String name, final LiveStats stats) {
            this.name = name;
            n = stats.num();
            min = (int)stats.minimum();
            max = (int)stats.maximum();
            mean = stats.mean();
            skewness = stats.skewness();
            kurtosis = stats.kurtosis();
            quantiles = ImmutableMap.copyOf(stats.quantiles());
        }
    }
}
