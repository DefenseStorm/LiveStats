package io.praesid.livestats;

import com.google.common.util.concurrent.AtomicDouble;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BooleanSupplier;
import java.util.function.DoubleUnaryOperator;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

public class LiveStatsTest {
    private static final Logger log = LogManager.getLogger();
    private static final double[] TEST_TILES = {.25,.5,.75,.9,.99,.999,.9999};
    private static final int SAMPLE_COUNT = 10000; // Lots of thresholds need tuning if this is changed
    private static final Stats expovarMaxPes =
            new Stats("", 0, 0, 0, .0000001, 5, .02, 100, quantileMaxPes(.2, .1, .05, .02, .01, .01, .01));
    private static final Stats knownMaxPes =
            new Stats("", 0, 0, 0, .0000001, 30, 5, 300, quantileMaxPes(5, 20, 50, 50, 100, 100, 100));
    private static final Stats gaussianMaxPes =
            new Stats("", 0, 0, 0, .0000001, .2, 2, 500, quantileMaxPes(.2, .1, .1, .2, 1, 5, 20));
    private static final Stats uniformMaxPes =
            new Stats("", 0, 0, 0, .0000001, .2, .05, 200, quantileMaxPes(10, 20, 20, 10, .02, .02, .05));
    private static final Stats triangularMaxPes =
            new Stats("", 0, 0, 0, .0000001, .2, .00001, 2, quantileMaxPes(.2, .5, .2, .2, .5, 1, 2));
    private static final Stats bimodalMaxPes =
            new Stats("", 0, 0, 0, .0000001, .5, .01, 1, quantileMaxPes(.5, .2, .2, .1, .2, .5, 1));

    @Test
    public void testKnown() { // Doesn't use SAMPLE_COUNT
        final double[] test = {0.02,0.15,0.74,3.39,0.83,22.37,10.15,15.43,38.62,15.92,34.60,
                               10.28,1.47,0.40,0.05,11.39,0.27,0.42,0.09,11.37};
        test("Test", Arrays.stream(test), knownMaxPes);
    }

    @Test
    public void testUniform() {
        final double[] ux = IntStream.range(0, SAMPLE_COUNT).asDoubleStream().toArray();
        Collections.shuffle(Arrays.asList(ux), ThreadLocalRandom.current()); // Shuffles the underlying array
        test("Uniform", Arrays.stream(ux), uniformMaxPes);
    }

    @Test
    public void testGaussian() {
        test("Gaussian", DoubleStream.generate(ThreadLocalRandom.current()::nextGaussian), gaussianMaxPes);
    }

    @Test
    public void testExpovar() {
        final double lambda = 1.0 / 435;
        test("Expovar", ThreadLocalRandom.current().doubles().map(d -> Math.log(1. - d) / lambda), expovarMaxPes);
    }

    @Test
    public void testTriangular() {
        final DoubleStream tx = ThreadLocalRandom.current().doubles()
                                                 .map(triangular(-100 * SAMPLE_COUNT, 100 * SAMPLE_COUNT, 100));
        test("Triangular", tx, triangularMaxPes);
    }

    @Test
    public void testBimodal() {
        final Random r = ThreadLocalRandom.current();
        final DoubleStream bx = r.doubles()
                                 .map(bimodal(r::nextBoolean, triangular(0, 1000, 500), triangular(500, 1500, 1400)));
        test("Bimodal", bx, bimodalMaxPes);
    }

    private void test(final String name, final DoubleStream dataStream, final Stats maxPes) {
        final double[] data = dataStream.limit(SAMPLE_COUNT).toArray();
        final LiveStats stats = new LiveStats(TEST_TILES);

        final long start = System.nanoTime();
        Arrays.stream(data).parallel().forEach(stats);
        final Stats live = new Stats(name, stats);

        final long mid = System.nanoTime();
        final Stats real = calculateReal(data, name);

        final long end = System.nanoTime();

        log.info("live ({}ns/datum): {}", (mid - start) / data.length, live);
        log.info("real ({}ns/datum): {}", (end - mid) / data.length, real);
        assertEquals("name", real.name, live.name);
        assertEquals("count", real.n, live.n);
        assertEquals("min", real.min, live.min, maxPes.min);
        assertEquals("max", real.max, live.max, maxPes.max);
        for (double tile : TEST_TILES) {
            assertEquals("p" + tile + "%e",
                         0.,
                         100 * (live.quantiles.get(tile) - real.quantiles.get(tile)) / (real.max - real.min),
                         maxPes.quantiles.get(tile));
        }
        assertEquals("mean%e", 0., 100 * (live.mean - real.mean) / real.mean, maxPes.mean);
        assertEquals("variance%e", 0., 100 * (live.variance - real.variance) / real.variance, maxPes.variance);
        assertEquals("skewness%e", 0., 100 * (live.skewness - real.skewness) / (real.max - real.min), maxPes.skewness);
        assertEquals("kurtosis%e", 0., 100 * (live.kurtosis - real.kurtosis) / real.kurtosis, maxPes.kurtosis);
    }

    private static DoubleUnaryOperator triangular(double low, double high, double mode) {
        final double pivot = (mode - low) / (high - low);
        return base -> {
            if (base <= pivot) {
                return low + Math.sqrt(base * (high - low) * (mode - low));
            } else {
                return high - Math.sqrt((1 - base) * (high - low) * (high - mode));
            }
        };
    }

    private static DoubleUnaryOperator bimodal(final BooleanSupplier toss,
                                               final DoubleUnaryOperator left, final DoubleUnaryOperator right) {
        return base -> toss.getAsBoolean() ? left.applyAsDouble(base) : right.applyAsDouble(base);
    }

    private static Map<Double, Double> quantileMaxPes(final double... maxPes) {
        return IntStream.range(0, TEST_TILES.length)
                        .collect(HashMap::new, (m, i) -> m.put(TEST_TILES[i], maxPes[i]), HashMap::putAll);
    }

    private static Stats calculateReal(final double[] data, final String name) {
        Arrays.sort(data);
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
        final double skew = u3 / Math.pow(u2, 3./2);
        final double kurt = u4 / Math.pow(u2, 2) - 3;

        return new Stats(name, data.length, data[0], data[data.length-1], avg, u2, skew, kurt, quantiles);
    }

}
