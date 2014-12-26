package io.praesid.livestats;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AtomicDouble;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

public class LiveStatsTest {
    private static final Logger log = LogManager.getLogger();
    private static final double[] TEST_TILES = {0.25,0.5,0.75};
    private static final int TEST_COUNT = 10000; // Lots of thresholds need tuning if this is changed
    private static final Stats expovarMaxPes =
            new Stats("", 0, 0, 0, .0000001, 1, .005, 10, quantileMaxPes(TEST_TILES, .5));
    private static final Stats knownMaxPes =
            new Stats("", 0, 0, 0, .0000001, 30, 5, 300, ImmutableMap.of(0.25, 5., 0.5, 20., 0.75, 20.));
    private static final Stats gaussianMaxPes =
            new Stats("", 0, 0, 0, .0000001, .2, 1, 50, quantileMaxPes(TEST_TILES, .1));
    private static final Stats uniformMaxPes =
            new Stats("", 0, 0, 0, .0000001, .2, .05, 200, quantileMaxPes(TEST_TILES, 15.));
    private static final Stats triangularMaxPes =
            new Stats("", 0, 0, 0, .0000001, .2, .00001, 2, quantileMaxPes(TEST_TILES, .2));
    private static final Stats bimodalMaxPes =
            new Stats("", 0, 0, 0, .0000001, .2, .01, 1, quantileMaxPes(TEST_TILES, .5));

    @Test
    public void testKnown() {
        final double[] test = {0.02,0.15,0.74,3.39,0.83,22.37,10.15,15.43,38.62,15.92,34.60,
                               10.28,1.47,0.40,0.05,11.39,0.27,0.42,0.09,11.37};
        test("Test", TEST_TILES, test, knownMaxPes);
    }

    @Test
    public void testUniform() {
        final double[] ux = IntStream.range(0, TEST_COUNT).asDoubleStream().toArray();
        Collections.shuffle(Arrays.asList(ux), ThreadLocalRandom.current()); // Shuffles the underlying array
        test("Uniform", TEST_TILES, ux, uniformMaxPes);
    }

    @Test
    public void testGaussian() {
        final double[] gx = DoubleStream.generate(ThreadLocalRandom.current()::nextGaussian).limit(TEST_COUNT)
                                        .toArray();
        test("Gaussian", TEST_TILES, gx, gaussianMaxPes);
    }

    @Test
    public void testExpovar() {
        final double[] ex = IntStream.range(0, TEST_COUNT).mapToDouble(i -> exponential(1.0 / 435)).toArray();
        test("Expovar", TEST_TILES, ex, expovarMaxPes);
    }

    @Test
    public void testTriangular() {
        final double[] tx = IntStream
                .range(0, TEST_COUNT).mapToDouble(i -> triangular(-100 * TEST_COUNT, 100 * TEST_COUNT, 100)).toArray();
        test("Triangular", TEST_TILES, tx, triangularMaxPes);
    }

    @Test
    public void testBimodal() {
        final double[] bx = IntStream
                .range(0, TEST_COUNT).mapToDouble(i -> bimodal(0, 1000, 500, 500, 1500, 1400)).toArray();
        test("Bimodal", TEST_TILES, bx, bimodalMaxPes);
    }

    private void test(final String name, final double[] tiles, final double[] data, final Stats maxPes) {
        final LiveStats stats = new LiveStats(tiles);

        final long start = System.nanoTime();
        Arrays.stream(data).parallel().forEach(stats);
        final Stats live = new Stats(name, stats);

        final long mid = System.nanoTime();
        final Stats real = calculateReal(tiles, data, name);

        final long end = System.nanoTime();

        log.info("live ({}ns/datum): {}", (mid - start) / TEST_COUNT, live);
        log.info("real ({}ns/datum): {}", (end - mid) / TEST_COUNT, real);
        assertEquals("name", real.name, live.name);
        assertEquals("count", real.n, live.n);
        assertEquals("min", real.min, live.min, maxPes.min);
        assertEquals("max", real.max, live.max, maxPes.max);
        for (double tile : tiles) {
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

    private static double triangular(double low, double high, double mode) {
        final double base = ThreadLocalRandom.current().nextDouble();
        final double pivot = (mode - low) / (high - low);
        if (base <= pivot) {
            return low + Math.sqrt(base * (high - low) * (mode - low));
        } else {
            return high - Math.sqrt((1 - base) * (high - low) * (high - mode));
        }
    }

    private static double bimodal(double low1, double high1, double mode1, double low2, double high2, double mode2) {
        final boolean toss = ThreadLocalRandom.current().nextBoolean();
        return toss ? triangular(low1, high1, mode1) : triangular(low2, high2, mode2);
    }

    private static double exponential(double lambda) {
        return Math.log(1 - ThreadLocalRandom.current().nextDouble()) / -lambda;
    }

    private static Map<Double, Double> quantileMaxPes(final double[] tiles, final double maxPe) {
        return Arrays.stream(tiles)
                     .mapToObj(x -> x)
                     .collect(Collectors.toMap(Function.identity(), ignored -> maxPe));
    }

    private static Stats calculateReal(final double[] tiles, final double[] data, final String name) {
        Arrays.sort(data);
        final Map<Double, Double> quantiles = Arrays
                .stream(tiles).mapToObj(x -> x)
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
