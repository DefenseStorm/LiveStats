package io.praesid.livestats;

import com.google.common.collect.ImmutableMap;
import lombok.ToString;

import java.util.Map;

@ToString
public final class Stats {
    public final String name;
    public final int n;
    public final double min;
    public final double max;
    public final double mean;
    public final double variance;
    public final double skewness;
    public final double kurtosis;
    public final Map<Double, Double> quantiles;

    public Stats(final String name, final LiveStats stats) {
        this.name = name;
        n = stats.num();
        min = stats.minimum();
        max = stats.maximum();
        mean = stats.mean();
        variance = stats.variance();
        skewness = stats.skewness();
        kurtosis = stats.kurtosis();
        quantiles = ImmutableMap.copyOf(stats.quantiles());
    }

    public Stats(final String name, final int n, final double min, final double max,
                 final double mean, final double variance,
                 final double skewness, final double kurtosis,
                 final Map<Double, Double> quantiles) {
        this.name = name;
        this.n = n;
        this.min = min;
        this.max = max;
        this.mean = mean;
        this.variance = variance;
        this.skewness = skewness;
        this.kurtosis = kurtosis;
        this.quantiles = ImmutableMap.copyOf(quantiles);
    }
}
