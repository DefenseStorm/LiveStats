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

    private final AtomicDouble sum = new AtomicDouble(0);
    private final AtomicDouble sumCentralMoment2 =  new AtomicDouble(0);
    private final AtomicDouble sumCentralMoment3 = new AtomicDouble(0);
    private final AtomicDouble sumCentralMoment4 = new AtomicDouble(0);
    private final AtomicInteger count = new AtomicInteger(0);
    private final ImmutableList<Quantile> quantiles;

    /**
     * Constructs a LiveStats object
     *
     * @param quantiles Quantiles to track (eg. .5 for the 50th percentile)
     */
    public LiveStats(final double... quantiles) {
        final Builder<Quantile> tilesBuilder = ImmutableList.builder();
        for (final double tile : quantiles) {
            tilesBuilder.add(new Quantile(tile));
        }
        this.quantiles = tilesBuilder.build();
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
        for (final Quantile quantile : quantiles) {
            quantile.add(item);
        }

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
        for (final Quantile quantile : quantiles) {
            builder.put(quantile.percentile, quantile.quantile());
        }
        return builder.build();
    }

    public double maximum() {
        return quantiles.get(0).maximum();
    }

    public double mean() {
        return sum.get() / count.get();
    }

    public double minimum() {
        return quantiles.get(0).minimum();
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
        private static final int N_MARKERS = 5; // positionDeltas and idealPositions must be updated if this is changed

        private final double[] positionDeltas; // Immutable, how far the ideal positions move for each item
        private final double[] idealPositions;
        private final int[] positions = {1, 2, 3, 4, 5};
        private final double[] heights = new double[N_MARKERS];
        private int initializedMarkers = 0;
        public final double percentile;

        /**
         * Constructs a single quantile object
         */
        public Quantile(double percentile) {
            this.percentile = percentile;
            positionDeltas = new double[]{percentile / 2, percentile, (1 + percentile) / 2, 1};
            idealPositions = new double[]{1 + 2 * percentile, 1 + 4 * percentile, 3 + 2 * percentile, 5};
        }

        public synchronized double minimum() {
            if (initializedMarkers < N_MARKERS) {
                Arrays.sort(heights);
            }
            return heights[0];
        }

        public synchronized double maximum() {
            if (initializedMarkers != N_MARKERS) {
                Arrays.sort(heights);
            }
            return heights[initializedMarkers - 1];
        }

        public synchronized double quantile() {
            if (initializedMarkers != N_MARKERS) {
                Arrays.sort(heights); // Not fully initialized, probably not in order
                // make sure we don't overflow on percentile == 1 or underflow on percentile == 0
                return heights[Math.min(Math.max(initializedMarkers - 1, 0), (int)(initializedMarkers * percentile))];
            }
            return heights[2];
        }

        /**
         * Adds another datum
         */
        public synchronized void add(double item) {
            if (initializedMarkers < N_MARKERS) {
                heights[initializedMarkers] = item;
                initializedMarkers++;
                if (initializedMarkers == N_MARKERS) {
                    Arrays.sort(heights);
                }
                return;
            }

            if (item > heights[N_MARKERS - 1]) {
                heights[N_MARKERS - 1] = item; // Marker N_MARKERS-1 is max
            } else if (item < heights[0]) {
                heights[0] = item; // Marker 0 is min
            }
            positions[N_MARKERS - 1]++; // Because marker N_MARKERS-1 is max, it always gets incremented
            for (int i = N_MARKERS - 2; heights[i] > item; i--) { // Increment all other markers > item
                positions[i]++;
            }

            for (int i = 0; i < idealPositions.length; i++) {
                idealPositions[i] += positionDeltas[i]; // updated desired positions
            }

            adjust();
        }

        private void adjust() {
            for (int i = 1; i < N_MARKERS - 1; i++) {
                final int position = positions[i];

                final double positionDelta = idealPositions[i - 1] - position;

                if ((positionDelta >= 1 && positions[i + 1] > position + 1) ||
                        (positionDelta <= -1 && positions[i - 1] < position - 1)) {
                    final int direction = positionDelta > 0 ? 1 : -1;

                    final double heightBelow = heights[i - 1];
                    final double height = heights[i];
                    final double heightAbove = heights[i + 1];
                    final int positionBelow = positions[i - 1];
                    final int positionAbove = positions[i + 1];
                    final double newHeight = calcP2(direction, heightBelow, height, heightAbove,
                                                    positionBelow, position, positionAbove);

                    if (heightBelow < newHeight && newHeight < heightAbove) {
                        heights[i] = newHeight;
                    } else {
                        // use linear form
                        final double rise = heights[i + direction] - height;
                        final int run = positions[i + direction] - position;
                        heights[i] = height + Math.copySign(rise / run, direction);
                    }

                    positions[i] = position + direction;
                }
            }
        }

        private static double calcP2(final /* d      */ int direction,
                                     final /* q(i-1) */ double heightBelow,
                                     final /* q(i)   */ double height,
                                     final /* q(i+1) */ double heightAbove,
                                     final /* n(i-1) */ int positionBelow,
                                     final /* n(i)   */ int position,
                                     final /* n(i+1) */ int positionAbove) {
            // q + d / (n(i+1) - n(i-1) *
            //     ((n - n(i-1) + d) * (q(i+1) - q) / (n(i+1) - n) + (n(i+1) - n - d) * (q - q(i-1)) / (n - n(i-1)))
            final int xBelow = position - positionBelow;
            final int xAbove = positionAbove - position;
            final double belowScale = (xAbove - direction) / (double)xBelow;
            final double aboveScale = (xBelow + direction) / (double)xAbove;
            final double lowerHalf = belowScale * (height - heightBelow);
            final double upperHalf = aboveScale * (heightAbove - height);
            return height + Math.copySign((upperHalf + lowerHalf) / (positionAbove - positionBelow), direction);
        }

    }
}
