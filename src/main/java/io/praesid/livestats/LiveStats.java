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

@ThreadSafe
@ToString
public final class LiveStats implements DoubleConsumer {

    private static final double[] DEFAULT_TILES = {0.5};

    private final AtomicDouble sum = new AtomicDouble(0);
    private final AtomicDouble sumCentralMoment2 =  new AtomicDouble(0);
    private final AtomicDouble sumCentralMoment3 = new AtomicDouble(0);
    private final AtomicDouble sumCentralMoment4 = new AtomicDouble(0);
    private final AtomicInteger count = new AtomicInteger(0);
    private final ImmutableList<Quantile> tiles;

    /**
     * Constructs a LiveStats object
     *
     * @param p An array of quantiles to track (default {0.5})
     */
    public LiveStats(final double... p) {
        final Builder<Quantile> tilesBuilder = ImmutableList.builder();
        final double[] tiles = p.length == 0 ? DEFAULT_TILES : p;
        for (final double tile : tiles) {
            tilesBuilder.add(new Quantile(tile));
        }
        this.tiles = tilesBuilder.build();
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
     */
    public void add(double item) {
        tiles.forEach(tile -> tile.add(item));

        final int myCount = count.incrementAndGet();

        final double mySum = sum.addAndGet(item);

        final double delta = item - mySum / myCount;

        final double delta2 = delta * delta;
        sumCentralMoment2.addAndGet(delta2);

        final double delta3 = delta2 * delta;
        sumCentralMoment3.addAndGet(delta3);

        final double delta4 = delta3 * delta;
        sumCentralMoment4.addAndGet(delta4);
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
        return sum.get() / count.get();
    }

    public double minimum() {
        return tiles.get(0).minimum();
    }

    public int num() {
        return count.get();
    }

    public double variance() {
        return sumCentralMoment2.get() / count.get();
    }

    public double kurtosis() {
        // u4 / u2^2 - 3
        // (s4/c) / (s2/c)^2 - 3
        // s4 / (c * (s2/c)^2) - 3
        // s4 / (c * (s2/c) * (s2/c)) - 3
        // s4 / (s2^2 / c) - 3
        // s4 * c / s2^2 - 3
        return sumCentralMoment4.get() * count.get() / Math.pow(sumCentralMoment2.get(), 2) - 3;
    }

    public double skewness() {
        // u3 / u2^(3/2)
        // (s3/c) / (s2/c)^(3/2)
        // s3 / (c * (s2/c)^(3/2))
        // s3 / (c * (s2/c) * (s2/c)^(1/2))
        // s3 / (s2 * sqrt(s2/c))
        // s3 * sqrt(c/s2) / s2
        final double mySumCentralMoment2 = sumCentralMoment2.get();
        return sumCentralMoment3.get() * Math.sqrt(count.get() / mySumCentralMoment2) / mySumCentralMoment2;
    }

    @ThreadSafe
    @ToString
    private static final class Quantile {
        private static final int N_MARKERS = 5; // dn and npos must be updated if this is changed

        private final double[] dn; // Immutable
        private final double[] npos;
        private final int[] pos = {1, 2, 3, 4, 5};
        private final double[] heights;
        private int initialized;
        public final double p;

        /**
         * Constructs a single quantile object
         */
        public Quantile(double p) {
            this.p = p;
            dn = new double[]{p / 2, p, (1 + p) / 2, 1};
            npos = new double[]{1 + 2 * p, 1 + 4 * p, 3 + 2 * p, 5};
            initialized = 0;
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

            for (int i = k; i < pos.length; i++) {
                pos[i]++; // increment all positions greater than k
            }

            for (int i = 0; i < npos.length; i++) {
                npos[i] += dn[i]; // updated desired positions
            }

            adjust();
        }

        private void adjust() {
            for (int i = 1; i < N_MARKERS - 1; i++) {
                final int n = pos[i];

                final double d0 = npos[i - 1] - n;

                if ((d0 >= 1 && pos[i + 1] > n + 1) || (d0 <= -1 && pos[i - 1] < n - 1)) {
                    final int d = d0 > 0 ? 1 : -1;

                    final double q = heights[i];
                    final double qp1 = heights[i + 1];
                    final double qm1 = heights[i - 1];
                    final int np1 = pos[i + 1];
                    final int nm1 = pos[i - 1];
                    final double qn = calcP2(d, q, qp1, qm1, n, np1, nm1);

                    if (qm1 < qn && qn < qp1) {
                        heights[i] = qn;
                    } else {
                        // use linear form
                        heights[i] = q + Math.copySign((heights[i + d] - q) / (pos[i + d] - n), d);
                    }

                    pos[i] = n + d;
                }
            }
        }

        private static double calcP2(int d, double q, double qp1, double qm1, int n, int np1, int nm1) {
            // q + d / (np1 - nm1) * ((n - nm1 + d) * (qp1 - q) / (np1 - n) + (np1 - n - d) * (q - qm1) / (n - nm1))
            final int leftX = n - nm1;
            final int rightX = np1 - n;
            final double rightScale = (leftX + d) / (double)rightX;
            final double leftScale = (rightX - d) / (double)leftX;
            final double leftHalf = leftScale * (q - qm1);
            final double rightHalf = rightScale * (qp1 - q);
            return q + Math.copySign((leftHalf + rightHalf) / (np1 - nm1), d);
        }

    }
}
