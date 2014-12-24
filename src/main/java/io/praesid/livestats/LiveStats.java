package io.praesid.livestats;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AtomicDouble;
import lombok.ToString;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.DoubleConsumer;
import java.util.stream.Collector;
import java.util.stream.IntStream;

@ThreadSafe
@ToString
public class LiveStats implements DoubleConsumer {

    private static final double[] DEFAULT_TILES = {0.5};

    private final AtomicDouble varM2 =  new AtomicDouble(0);
    private final AtomicDouble kurtM4 = new AtomicDouble(0);
    private final AtomicDouble skewM3 = new AtomicDouble(0);
    private final AtomicDouble average = new AtomicDouble(0);
    private final AtomicInteger count = new AtomicInteger(0);
    private final ImmutableList<Quantile> tiles;

    /**
     * Constructs a LiveStats object
     *
     * @param p A list of quantiles to track (default {0.5})
     */
    public LiveStats(final double... p) {
        tiles = Arrays.stream(p.length == 0 ? DEFAULT_TILES : p)
                      .mapToObj(Quantile::new)
                      .collect(Collector.of(ImmutableList::<Quantile>builder,
                                            Builder::add,
                                            (a, b) -> a.addAll(b.build()),
                                            Builder::build));
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
    public void add(double item) {
        tiles.forEach(tile -> tile.add(item));

        final int myCount = count.incrementAndGet();

        final double preDelta = item - average.get();
        // This is wrong if it matters that post delta is relative to a different point in "time" than the pre delta
        final double postDelta = item - average.addAndGet(preDelta / myCount);

        final double d2 = postDelta * postDelta;
        //Variance(except for the scale)
        varM2.addAndGet(d2);

        final double d3 = d2 * postDelta;
        //Skewness
        skewM3.addAndGet(d3);

        final double d4 = d3 * postDelta;
        //Kurtosis
        kurtM4.addAndGet(d4);
    }

    /**
     * @return a Map of quantile to approximate value
     */
    public Map<Double, Double> quantiles() {
        final ImmutableMap.Builder<Double, Double> builder = ImmutableMap.builder();
        for (final Quantile tile : tiles) {
            builder.put(tile.p, tile.quantile());
        }
        return builder.build();
    }

    public double maximum() {
        return tiles.get(0).maximum();
    }

    public double mean() {
        return average.get();
    }

    public double minimum() {
        return tiles.get(0).minimum();
    }

    public int num() {
        return count.get();
    }

    public double variance() {
        return varM2.get() / count.get();
    }

    public double kurtosis() {
        // k / (c * (v/c)^2) - 3
        // k / (c * (v/c) * (v/c)) - 3
        // k / (v * v / c) - 3
        // k * c / (v * v) - 3
        final double myVarM2 = varM2.get();
        return kurtM4.get() * count.get() / (myVarM2 * myVarM2) - 3;
    }

    public double skewness() {
        // s / (c * (v/c)^(3/2))
        // s / (c * (v/c) * (v/c)^(1/2))
        // s / (v * sqrt(v/c))
        // s * sqrt(c/v) / v
        final double myVarM2 = varM2.get();
        return skewM3.get() * Math.sqrt(count.get() / myVarM2) / myVarM2;
    }

    @ThreadSafe
    @ToString
    private static class Quantile {
        private static final int N_MARKERS = 5; // dn and npos must be updated if this is changed

        private final double[] dn; // Immutable
        private final double[] npos;
        private final int[] pos;
        private final double[] heights;
        private int initialized = 0;
        public final double p;

        /**
         * Constructs a single quantile object
         */
        public Quantile(double p) {
            this.p = p;
            dn = new double[]{0, p / 2, p, (1 + p) / 2, 1};
            npos = new double[]{1, 1 + 2 * p, 1 + 4 * p, 3 + 2 * p, 5};
            pos = IntStream.range(1, N_MARKERS + 1).toArray();
            heights = new double[N_MARKERS];
        }

        public synchronized double minimum() {
            if (initialized < N_MARKERS) {
                Arrays.sort(heights);
            }
            return heights[0];
        }

        public synchronized double maximum() {
            if (initialized != N_MARKERS) {
                Arrays.sort(heights);
            }
            return heights[initialized - 1];
        }

        public synchronized double quantile() {
            if (initialized != N_MARKERS) {
                Arrays.sort(heights); // Not fully initialized, probably not in order
                // make sure we don't overflow on p == 1 or underflow on p == 0
                return heights[Math.min(Math.max(initialized - 1, 0), (int)(initialized * p))];
            }
            return heights[2];
        }

        /**
         * Adds another datum
         */
        public synchronized void add(double item) {
            if (initialized < N_MARKERS) {
                heights[initialized] = item;
                initialized++;
                if (initialized == N_MARKERS) {
                    Arrays.sort(heights);
                }
                return;
            }

            // find cell k
            final int k;
            if (item < heights[0]) {
                heights[0] = item;
                k = 1;
            } else if (item >= heights[N_MARKERS - 1]) {
                heights[N_MARKERS - 1] = item;
                k = N_MARKERS - 1;
            } else {
                int i = 1; // Linear search is fastest because N_MARKERS is small
                while (item >= heights[i]) {
                    i++;
                }
                k = i;
            }

            IntStream.range(k, pos.length).forEach(i -> pos[i]++); // increment all positions greater than k

            IntStream.range(1, npos.length).forEach(i -> npos[i] += dn[i]);

            adjust();
        }

        private void adjust() {
            for (int i = 1; i < N_MARKERS - 1; i++) {
                final int n = pos[i];
                final double q = heights[i];

                final double d0 = npos[i] - n;

                if ((d0 >= 1 && pos[i + 1] > n + 1) || (d0 <= -1 && pos[i - 1] < n - 1)) {
                    final int d = (int)Math.signum(d0);

                    final double qp1 = heights[i + 1];
                    final double qm1 = heights[i - 1];
                    final int np1 = pos[i + 1];
                    final int nm1 = pos[i - 1];
                    final double qn = calcP2(d, q, qp1, qm1, n, np1, nm1);

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

        private static double calcP2(int d, double q, double qp1, double qm1, double n, double np1, double nm1) {
            final double outer = d / (np1 - nm1);
            final double innerLeft = (n - nm1 + d) * (qp1 - q) / (np1 - n);
            final double innerRight = (np1 - n - d) * (q - qm1) / (n - nm1);

            return q + outer * (innerLeft + innerRight);
        }

    }
}
