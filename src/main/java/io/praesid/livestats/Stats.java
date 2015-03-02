package io.praesid.livestats;

import com.google.common.collect.ImmutableMap;
import lombok.ToString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Optional;

@ToString
public class Stats {
    private static final Logger log = LogManager.getLogger();

    public final String name;
    public final int decays;
    public final long n;
    public final Double decayedN;
    public final Double min;
    public final Double decayedMin;
    public final Double max;
    public final Double decayedMax;
    public final Double mean;
    public final Double variance;
    public final Double skewness;
    public final Double kurtosis;
    public final Map<Double, Double> quantiles;

    public Stats(final String name, final LiveStats stats) {
        this.name = name;
        n = stats.num();
        min = specialFloatsToNull(name, "min", stats.minimum());
        max = specialFloatsToNull(name, "max", stats.maximum());
        mean = specialFloatsToNull(name, "mean", stats.mean());
        variance = specialFloatsToNull(name, "variance", stats.variance());
        skewness = specialFloatsToNull(name, "skewness", stats.skewness());
        kurtosis = specialFloatsToNull(name, "kurtosis", stats.kurtosis());
        final ImmutableMap.Builder<Double, Double> quantilesBuilder = ImmutableMap.builder();
        stats.quantiles.forEach(q -> Optional.ofNullable(specialFloatsToNull(name, "" + q.percentile,
                                                                             q.quantile()))
                                             .ifPresent(quantile -> quantilesBuilder.put(q.percentile, quantile)));
        quantiles = quantilesBuilder.build();
        decayedMin = specialFloatsToNull(name, "decayedMin", stats.decayedMinimum());
        decayedMax = specialFloatsToNull(name, "decayedMax", stats.decayedMaximum());
        decayedN = specialFloatsToNull(name, "decayedN", stats.decayedNum());
        decays = stats.decayCount();
    }

    public Stats(final String name, final long n, final double min, final double max,
                 final double mean, final double variance,
                 final double skewness, final double kurtosis,
                 final Map<Double, Double> quantiles) {
        this.name = name;
        this.decays = 0;
        this.n = n;
        this.decayedN = (double)n;
        this.min = min;
        this.decayedMin = min;
        this.max = max;
        this.decayedMax = max;
        this.mean = mean;
        this.variance = variance;
        this.skewness = skewness;
        this.kurtosis = kurtosis;
        this.quantiles = ImmutableMap.copyOf(quantiles);
    }

    @SuppressWarnings("ReturnOfNull")
    private static Double specialFloatsToNull(final String name, final String var, final double value) {
        if (Double.isNaN(value)) {
            log.debug("{}: {} is NaN", name, var);
            return null;
        }
        if (Double.isInfinite(value)) {
            log.debug("{}: {} is infinite", name, var);
            return null;
        }
        return value;
    }
}
