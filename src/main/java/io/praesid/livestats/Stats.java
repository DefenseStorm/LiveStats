package io.praesid.livestats;

import com.google.common.collect.ImmutableMap;
import lombok.ToString;

import java.util.Map;

@ToString
public final class Stats {
    public final String name;
    public final int n;
    public final int min;
    public final int max;
    public final double mean;
    public final double variance;
    public final double skewness;
    public final double kurtosis;
    public final Map<Double, Double> quantiles;

    public Stats(final String name, final LiveStats stats) {
        this.name = name;
        n = stats.num();
        min = (int)stats.minimum();
        max = (int)stats.maximum();
        mean = stats.mean();
        this.variance = stats.variance();
        skewness = stats.skewness();
        kurtosis = stats.kurtosis();
        quantiles = ImmutableMap.copyOf(stats.quantiles());
    }
}
