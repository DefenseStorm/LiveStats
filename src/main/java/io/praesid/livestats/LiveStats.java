package io.praesid.livestats;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AtomicDouble;

import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.DoubleConsumer;
import java.util.function.DoublePredicate;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@ThreadSafe
public class LiveStats implements DoublePredicate, DoubleConsumer {

    private AtomicDouble minVal = new AtomicDouble(Double.MAX_VALUE);
    private AtomicDouble maxVal = new AtomicDouble(Double.MIN_VALUE);
    private double varM2 = 0.;
    private double kurtM4 = 0.;
    private double skewM3 = 0.;
    private int fullStatsCount = 0;
    private double average = 0.;
    private AtomicInteger count = new AtomicInteger(0);
    private final double fullStatsProbability;
    private final Quantile[] tiles;

    /**
     * Constructs a LiveStats object
     *
     * @param fullStatsProbability the probability that any given item is considered for all available statistics
     *                             other items are only counted and considered for maximum and minimum.
     *                             values &lt; 0 disable full stats and values &gt; 1 always calculate full stats
     * @param p A list of quantiles to track (default {0.5})
     */
    public LiveStats(final double fullStatsProbability, final double... p) {
        this.fullStatsProbability = fullStatsProbability;
        tiles = Arrays.stream(p.length == 0 ? new double[]{0.5} : p).mapToObj(Quantile::new).toArray(Quantile[]::new);
    }

    /**
     * Adds another datum
     *
     * @param item the value to add
     */
    @Override
    public void accept(final double item) {
        add(item);
    }

    /**
     * Adds another datum
     *
     * @param item the value to add
     * @return true if full stats were collected, false otherwise
     */
    @Override
    public boolean test(final double item) {
        return add(item);
    }

    /**
     * Adds another datum
     *
     * @param item the value to add
     * @return true if full stats were collected, false otherwise
     */
    public boolean add(double item) {
        double oldMin = minVal.get();
        while (item < oldMin) {
            if (minVal.compareAndSet(oldMin, item)) {
                break;
            }
            oldMin = minVal.get();
        }
        double oldMax = maxVal.get();
        while (item > oldMax) {
            if (maxVal.compareAndSet(oldMax, item)) {
                break;
            }
            oldMax = maxVal.get();
        }
        count.incrementAndGet();

        if (ThreadLocalRandom.current().nextDouble() < fullStatsProbability) {
            synchronized(this) {
                fullStatsCount++;

                //tiles
                for (final Quantile tile : tiles) {
                    tile.add(item);
                }

                final double preDelta = item - average;
                //Average
                average += preDelta / fullStatsCount;


                final double postDelta = item - average;
                //Variance(except for the scale)
                varM2 += preDelta * postDelta;

                final double cubedPostDelta = Math.pow(postDelta, 3);
                //Skewness
                skewM3 += cubedPostDelta;

                //Kurtosis
                kurtM4 += cubedPostDelta * postDelta;

                return true;
            }
        }
        return false;
    }

    /**
     * @return a Map of quantile to approximate value
     */
    public synchronized Map<Double, Double> quantiles() {
        final ImmutableMap.Builder<Double, Double> builder = ImmutableMap.builder();
        for (final Quantile tile : tiles) {
            builder.put(tile.p, tile.quantile());
        }
        return builder.build();
    }

    public double maximum() {
        return maxVal.get();
    }

    public double mean() {
        return average;
    }

    public double minimum() {
        return minVal.get();
    }

    public int num() {
        return count.get();
    }

    public synchronized double variance() {
        if (fullStatsCount > 1) {
            return varM2 / fullStatsCount;
        }
        return Double.NaN;
    }

    public synchronized double kurtosis() {
        if (fullStatsCount > 1) {
            return kurtM4 / (fullStatsCount * Math.pow(variance(), 2)) - 3;
        }
        return Double.NaN;
    }

    public synchronized double skewness() {
        if (fullStatsCount > 1) {
            return skewM3 / (fullStatsCount * Math.pow(variance(), 1.5));
        }
        return Double.NaN;
    }

    @NotThreadSafe
    private static class Quantile {
        private static final int LEN = 5; // dn and npos must be updated if this is changed

        private final double[] dn; // Immutable
        private final double[] npos;
        private final int[] pos;
        private final double[] heights;
        private int initialized = LEN;
        public final double p;

        /**
         * Constructs a single quantile object
         */
        public Quantile(double p) {
            this.p = p;
            dn = new double[]{0, p / 2, p, (1 + p) / 2, 1};
            npos = new double[]{1, 1 + 2 * p, 1 + 4 * p, 3 + 2 * p, 5};
            pos = IntStream.range(1, LEN + 1).toArray();
            heights = new double[LEN];
        }

        /**
         * Adds another datum
         */
        public void add(double item) {
            if (initialized < LEN) {
                heights[initialized] = item;
                initialized++;
                if (initialized == LEN) {
                    Arrays.sort(heights);
                }
                return;
            }
            // find cell k
            final int k;
            if (item < heights[0]) {
                heights[0] = item;
                k = 1;
            } else if (item >= heights[LEN - 1]) {
                heights[LEN - 1] = item;
                k = 4;
            } else {
                int i = 1; // Linear search is fastest for small LEN
                while (item >= heights[i]) {
                    i++;
                }
                k = i;
            }

            IntStream.range(k, pos.length).forEach(i -> pos[i]++); // increment all positions greater than k

            IntStream.range(0, npos.length).forEach(i -> npos[i] += dn[i]);

            adjust();
        }

        private void adjust() {
            for (int i = 1; i < LEN - 1; i++) {
                final int n = pos[i];
                final double q = heights[i];

                final double d0 = npos[i] - n;

                if ((d0 >= 1 && pos[i + 1] - n > 1) || (d0 <= -1 && pos[i - 1] - n < -1)) {
                    final int d = (int)Math.signum(d0);

                    final double qp1 = heights[i + 1];
                    final double qm1 = heights[i - 1];
                    final int np1 = pos[i + 1];
                    final int nm1 = pos[i - 1];
                    final double qn = calcP2(qp1, q, qm1, d, np1, n, nm1);

                    if (qm1 < qn && qn < qp1) {
                        heights[i] = qn;
                    } else {
                        // use linear form
                        heights[i] = q + d * (heights[i + d] - q) / (pos[i + d] - n);
                    }

                    pos[i] = n + d;
                }
            }
        }

        public double quantile() {
            if (initialized == LEN) {
                return heights[2];
            }
            Arrays.sort(heights);
            // make sure we don't overflow on p == 1 or underflow on p == 0
            return heights[Math.min(Math.max(initialized - 1, 0), (int)(initialized * p))];
        }

    }

    private static double calcP2(double qp1, double q, double qm1, double d, double np1, double n, double nm1) {
        final double outer = d / (np1 - nm1);
        final double innerLeft = (n - nm1 + d) * (qp1 - q) / (np1 - n);
        final double innerRight = (np1 - n - d) * (q - qm1) / (n - nm1);

        return q + outer * (innerLeft + innerRight);
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

    private static void output(final double[] data, final LiveStats stats, final String name) {
        Arrays.sort(data);
        final Map<Double, Double> tiles = stats.quantiles();
        final Map<Double, Double> expected = tiles
                .keySet().stream().collect(Collectors.toMap(Function.identity(), x -> data[(int)(data.length * x)]));
        final double et = expected
                .entrySet().stream()
                .mapToDouble(e -> Math.abs(tiles.get(e.getKey()) - e.getValue()) / Math.abs(e.getValue()))
                .sum();
        final double pe = 100 * et / data.length;
        final double avg = Arrays.stream(data).sum() / data.length;

        final double s2 = Arrays.stream(data).map(x -> Math.pow(x - avg, 2)).sum();
        final double var = s2 / data.length;

        final double v_pe = 100.0 * Math.abs(stats.variance() - var) / Math.abs(var);
        final double avg_pe = 100.0 * Math.abs(stats.mean() - avg) / Math.abs(avg);

        System.out.println(String.format("%s: Avg%%E %e Var%%E %e Quant%%E %e, Kurtosis %e, Skewness %e",
                                         name, avg_pe, v_pe, pe, stats.kurtosis(), stats.skewness()));
    }

    public static void main(String... args) {
        final int count = Integer.parseInt(args[0]);

        final double[] tiles = {0.25,0.5,0.75};

        final LiveStats median = new LiveStats(1, tiles);
        final double[] test = {0.02,0.15,0.74,3.39,0.83,22.37,10.15,15.43,38.62,15.92,34.60,
                               10.28,1.47,0.40,0.05,11.39,0.27,0.42,0.09,11.37};
        Arrays.stream(test).forEach(median);
        output(test, median, "Test");

        final LiveStats uniform = new LiveStats(1, tiles);
        final LiveStats uniform50 = new LiveStats(.5, tiles);
        final double[] ux = IntStream.range(0, count).asDoubleStream().toArray();
        Collections.shuffle(Arrays.asList(ux), ThreadLocalRandom.current()); // Shuffles the underlying array
        Arrays.stream(ux).peek(uniform).forEach(uniform50);
        output(ux, uniform, "Uniform");
        output(ux, uniform50, "Uniform50");

        final LiveStats expovar = new LiveStats(1, tiles);
        final double[] ex = IntStream.range(0, count)
                                     .mapToDouble(i -> exponential(1.0 / 435))
                                     .peek(expovar).toArray();
        output(ex, expovar, "Expovar");

        final LiveStats triangular = new LiveStats(1, tiles);
        final double[] tx = IntStream.range(0, count)
                                     .mapToDouble(i -> triangular(-1000 * count / 10, 1000 * count / 10, 100))
                                     .peek(triangular).toArray();
        output(tx, triangular, "Triangular");

        final LiveStats bimodal = new LiveStats(1, tiles);
        final LiveStats bimodal50 = new LiveStats(.5, tiles);
        final double[] bx = IntStream.range(0, count)
                                     .mapToDouble(i -> bimodal(0, 1000, 500, 500, 1500, 1400))
                                     .peek(bimodal).peek(bimodal50).toArray();
        output(bx, bimodal, "Bimodal");
        output(bx, bimodal50, "Bimodal50");
    }

}
