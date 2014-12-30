package io.praesid.livestats;

import com.google.common.util.concurrent.AtomicDouble;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BooleanSupplier;
import java.util.function.DoubleUnaryOperator;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

public abstract class LiveStatsTestBase {
    protected static final double[] TEST_TILES = {.25,.5,.75,.9,.99,.999,.9999};

    protected DoubleUnaryOperator triangular(double low, double high, double mode) {
        final double pivot = (mode - low) / (high - low);
        return base -> {
            if (base <= pivot) {
                return low + Math.sqrt(base * (high - low) * (mode - low));
            } else {
                return high - Math.sqrt((1 - base) * (high - low) * (high - mode));
            }
        };
    }

    protected DoubleUnaryOperator bimodal(final BooleanSupplier toss,
                                          final DoubleUnaryOperator left, final DoubleUnaryOperator right) {
        return base -> toss.getAsBoolean() ? left.applyAsDouble(base) : right.applyAsDouble(base);
    }

    protected static Map<Double, Double> quantileMaxPes(final double... maxPes) {
        return IntStream.range(0, TEST_TILES.length)
                        .collect(HashMap::new, (m, i) -> m.put(TEST_TILES[i], maxPes[i]), HashMap::putAll);
    }

    protected double calculateError(final double live, final double real, final double denominator) {
        if (live == real) {
            return 0;
        }
        if (denominator == 0) {
            return Double.POSITIVE_INFINITY;
        }
        return 100 * (live - real) / denominator;
    }

    protected Stats calculateReal(final String name, final DoubleStream dataStream) {
        final double[] data = dataStream.sorted().toArray();
        final Map<Double, Double> quantiles = Arrays
                .stream(TEST_TILES).mapToObj(x -> x)
                .collect(Collectors.toMap(Function.identity(), x -> data[(int)(data.length * x)]));

        final double avg = Arrays.stream(data).parallel().sum() / data.length;

        final AtomicDouble s2 = new AtomicDouble(0);
        final AtomicDouble s3 = new AtomicDouble(0);
        final AtomicDouble s4 = new AtomicDouble(0);
        Arrays.stream(data).parallel().forEach(x -> {
            s2.addAndGet(Math.pow(x - avg, 2));
            s3.addAndGet(Math.pow(x - avg, 3));
            s4.addAndGet(Math.pow(x - avg, 4));
        });

        final double u2 = s2.get() / data.length;
        final double u3 = s3.get() / data.length;
        final double u4 = s4.get() / data.length;
        final double skew = u3 == 0 ? 0 : (u3 / Math.pow(u2, 3./2));
        final double kurt = u4 == 0 ? 0 : (u4 / Math.pow(u2, 2) - 3);

        return new Stats(name, data.length, data[0], data[data.length-1], avg, u2, skew, kurt, quantiles);
    }
}
