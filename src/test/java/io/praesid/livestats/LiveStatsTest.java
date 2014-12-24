package io.praesid.livestats;

import com.google.common.util.concurrent.AtomicDouble;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

public class LiveStatsTest {
    private static final Logger log = LogManager.getLogger();
    private static final double[] TEST_TILES = {0.25,0.5,0.75};
    private static final int TEST_COUNT = 100000;

    @Ignore
    @Test
    public void testKnown() {
        final double[] test = {0.02,0.15,0.74,3.39,0.83,22.37,10.15,15.43,38.62,15.92,34.60,
                               10.28,1.47,0.40,0.05,11.39,0.27,0.42,0.09,11.37};
        test("Test", TEST_TILES, 1, test);
    }

    @Test
    public void testUniform() {
        final double[] ux = IntStream.range(0, TEST_COUNT).asDoubleStream().toArray();
        Collections.shuffle(Arrays.asList(ux), ThreadLocalRandom.current()); // Shuffles the underlying array
        test("Uniform", TEST_TILES, 1, ux);
    }

    @Test
    public void testUniform50() {
        final double[] ux = IntStream.range(0, TEST_COUNT).asDoubleStream().toArray();
        Collections.shuffle(Arrays.asList(ux), ThreadLocalRandom.current()); // Shuffles the underlying array
        test("Uniform50", TEST_TILES, .5, ux);
    }

    @Test
    public void testExpovar() {
        final double[] ex = IntStream.range(0, TEST_COUNT).mapToDouble(i -> exponential(1.0 / 435)).toArray();
        test("Expovar", TEST_TILES, 1, ex);
    }

    @Test
    public void testTriangular() {
        final double[] tx = IntStream.range(0, TEST_COUNT)
                                     .mapToDouble(i -> triangular(-1000 * TEST_COUNT / 10, 1000 * TEST_COUNT / 10, 100))
                                     .toArray();
        test("Triangular", TEST_TILES, 1, tx);
    }

    @Test
    public void testBimodal() {
        final double[] bx = IntStream.range(0, TEST_COUNT)
                                     .mapToDouble(i -> bimodal(0, 1000, 500, 500, 1500, 1400))
                                     .toArray();
        test("Bimodal", TEST_TILES, 1, bx);
    }

    @Test
    public void testBimodal50() {
        final double[] bx = IntStream.range(0, TEST_COUNT)
                                     .mapToDouble(i -> bimodal(0, 1000, 500, 500, 1500, 1400))
                                     .toArray();
        test("Bimodal50", TEST_TILES, .5, bx);
    }

    private void test(final String name, final double[] tiles, final double fullStatsProbability, final double[] data) {
        final LiveStats stats = new LiveStats(fullStatsProbability, tiles);

        final long start = System.nanoTime();
        Arrays.stream(data).parallel().forEach(stats);
        final Stats live = new Stats(name, stats);

        final long mid = System.nanoTime();
        final Stats real = calculateReal(tiles, data, name);

        final long end = System.nanoTime();

        log.info("live ({}us): {}", (mid - start) / 1000, live);
        log.info("real ({}us): {}", (end - mid) / 1000, real);
        assertEquals("name", real.name, live.name);
        assertEquals("count", real.n, live.n);
        assertEquals("min", real.min, live.min, Math.ulp(real.min));
        assertEquals("max", real.max, live.max, Math.ulp(real.max));
        for (double tile : tiles) {
            assertEquals("p" + tile + "PE",
                         0.,
                         100 * (live.quantiles.get(tile) - real.quantiles.get(tile)) / real.quantiles.get(tile),
                         1.);
        }
        assertEquals("meanPE", 0., 100 * (live.mean - real.mean) / real.mean, .1);
        assertEquals("variancePE", 0., 100 * (live.variance - real.variance) / real.variance, 1.);
        assertEquals("skewnessPE", 0., 100 * (live.skewness - real.skewness) / real.skewness, 1.);
        assertEquals("kurtosisPE", 0., 100 * (live.kurtosis - real.kurtosis) / real.kurtosis, 1.);
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

        final double skew = Math.sqrt(data.length) * s3.get() / Math.pow(s2.get(), 3./2);
        final double kurt = data.length * s4.get() / Math.pow(s2.get(), 2) - 3;
        final double var = s2.get() / data.length;

        return new Stats(name, data.length, data[0], data[data.length-1], avg, var, skew, kurt, quantiles);
    }

}
