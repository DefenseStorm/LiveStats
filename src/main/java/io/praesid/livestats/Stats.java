package io.praesid.livestats;

import com.google.common.collect.ImmutableMap;
import lombok.ToString;

import java.util.Map;

@ToString
public class Stats {
    public final String name;
    public final int decays;
    public final long n;
    public final double decayedN;
    public final double min;
    public final double decayedMin;
    public final double max;
    public final double decayedMax;
    public final Double mean;
    public final Double variance;
    public final Double skewness;
    public final Double kurtosis;
    public final Map<Double, Double> quantiles;

    public Stats(final String name, final LiveStats stats) {
        this.name = name;
        n = stats.num();
        min = stats.minimum();
        max = stats.maximum();
        mean = nanToNull(stats.mean());
        variance = nanToNull(stats.variance());
        skewness = nanToNull(stats.skewness());
        kurtosis = nanToNull(stats.kurtosis());
        quantiles = ImmutableMap.copyOf(stats.quantiles());
        decayedMin = stats.decayedMinimum();
        decayedMax = stats.decayedMaximum();
        decayedN = stats.decayedNum();
        decays = stats.decayCount();
    }

    public Stats(final String name, final long n, final double min, final double max,
                 final double mean, final double variance,
                 final double skewness, final double kurtosis,
                 final Map<Double, Double> quantiles) {
        this.name = name;
        this.decays = 0;
        this.n = n;
        this.decayedN = n;
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

    private static Double nanToNull(final double value) {
        //noinspection ReturnOfNull
        return Double.isNaN(value) ? null : value;
    }
}
