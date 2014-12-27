package io.praesid.livestats;

import java.util.Map;
import java.util.function.DoubleConsumer;

public interface LiveStats extends DoubleConsumer {
    void add(double item);
    Map<Double, Double> quantiles();
    double maximum();
    double mean();
    double minimum();
    int num();
    double variance();
    double kurtosis();
    double skewness();
}
