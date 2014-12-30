package io.praesid.livestats;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableMap;
import lombok.ToString;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;

@ThreadSafe
@ToString
public final class DecayingLiveStats implements LiveStats {

    @GuardedBy("this")
    private double min = Double.POSITIVE_INFINITY;
    @GuardedBy("this")
    private double max = Double.NEGATIVE_INFINITY;
    @GuardedBy("this")
    private double sum = 0;
    @GuardedBy("this")
    private double sumCentralMoment2 =  0;
    @GuardedBy("this")
    private double sumCentralMoment3 = 0;
    @GuardedBy("this")
    private double sumCentralMoment4 = 0;
    private int count = 0;
    @GuardedBy("this")
    private double decayedCount = 0;
    @GuardedBy("this")
    private int decayCount = 0;
    private final ImmutableList<Quantile> quantiles;
    private final long startNanos = System.nanoTime();
    private final double decayMultiplier;
    private final long decayPeriodNanos;

    /**
     * Constructs a LiveStats object
     *
     * @param quantiles Quantiles to track (eg. .5 for the 50th percentile)
     */
    public DecayingLiveStats(final double decayRatio, final Duration decayPeriod, final double... quantiles) {
        final Builder<Quantile> tilesBuilder = ImmutableList.builder();
        for (final double tile : quantiles) {
            tilesBuilder.add(new Quantile(tile));
        }
        this.quantiles = tilesBuilder.build();
        decayMultiplier = 1 - decayRatio;
        decayPeriodNanos = decayPeriod.toNanos();
    }

    public void decay() {
        final int expectedDecays = (int)((System.nanoTime() - startNanos) / decayPeriodNanos);
        final double myDecayMultiplier;
        synchronized (this) {
            if (expectedDecays > decayCount) {
                myDecayMultiplier = Math.pow(decayMultiplier, expectedDecays - decayCount);
                sum *= myDecayMultiplier;
                decayedCount *= myDecayMultiplier;
                sumCentralMoment2 *= myDecayMultiplier;
                sumCentralMoment3 *= myDecayMultiplier;
                sumCentralMoment4 *= myDecayMultiplier;
                decayCount = expectedDecays;
            } else {
                myDecayMultiplier = 1;
            }
        }

        for (final Quantile quantile : quantiles) {
            quantile.decay(myDecayMultiplier);
        }
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
        decay();

        synchronized(this) {
            if (item < min) {
                min = item;
            }
            if (item > max) {
                max = item;
            }
            count++;
            decayedCount++;
            sum += item;
            final double delta = item - sum / decayedCount;

            final double delta2 = delta * delta;
            sumCentralMoment2 += delta2;

            final double delta3 = delta2 * delta;
            sumCentralMoment3 += delta3;

            final double delta4 = delta3 * delta;
            sumCentralMoment4 += delta4;
        }

        for (final Quantile quantile : quantiles) {
            quantile.add(item);
        }
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

    public synchronized double maximum() {
        return max;
    }

    public double decayedMaximum() {
        return quantiles.stream().mapToDouble(Quantile::maximum).max().getAsDouble();
    }

    public synchronized double mean() {
        return sum / decayedCount;
    }

    public synchronized double minimum() {
        return min;
    }

    public double decayedMinimum() {
        return quantiles.stream().mapToDouble(Quantile::minimum).min().getAsDouble();
    }

    public synchronized int num() {
        return count;
    }

    public synchronized double decayedNum() {
        return decayedCount;
    }

    public synchronized int decayCount() {
        return decayCount;
    }

    public synchronized double variance() {
        return sumCentralMoment2 / decayedCount;
    }

    public synchronized double kurtosis() {
        // u4 / u2^2 - 3
        // (s4/c) / (s2/c)^2 - 3
        // s4 / (c * (s2/c)^2) - 3
        // s4 / (c * (s2/c) * (s2/c)) - 3
        // s4 / (s2^2 / c) - 3
        // s4 * c / s2^2 - 3
        if (sumCentralMoment4 == 0) {
            return 0;
        }
        return sumCentralMoment4 * decayedCount / Math.pow(sumCentralMoment2, 2) - 3;
    }

    public synchronized double skewness() {
        // u3 / u2^(3/2)
        // (s3/c) / (s2/c)^(3/2)
        // s3 / (c * (s2/c)^(3/2))
        // s3 / (c * (s2/c) * (s2/c)^(1/2))
        // s3 / (s2 * sqrt(s2/c))
        // s3 * sqrt(c/s2) / s2
        if (sumCentralMoment3 == 0) {
            return 0;
        }
        return sumCentralMoment3 * Math.sqrt(decayedCount / sumCentralMoment2) / sumCentralMoment2;
    }

    @ThreadSafe
    @ToString
    private static final class Quantile {
        private static final int N_MARKERS = 5; // positionDeltas and idealPositions must be updated if this is changed

        // length of positionDeltas and idealPositions is N_MARKERS-1 because the lowest idealPosition is always 1
        private final double[] positionDeltas; // Immutable, how far the ideal positions move for each item
        private final double[] idealPositions;
        private final double[] positions = {1, 2, 3, 4, 5};
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
                Arrays.sort(heights, 0, initializedMarkers);
            }
            return heights[0];
        }

        public synchronized double maximum() {
            if (initializedMarkers < N_MARKERS) {
                Arrays.sort(heights, 0, initializedMarkers);
            }
            return heights[initializedMarkers - 1];
        }

        public synchronized double quantile() {
            if (initializedMarkers < N_MARKERS) {
                Arrays.sort(heights, 0, initializedMarkers); // Not fully initialized, probably not in order
                // make sure we don't overflow on percentile == 1 or underflow on percentile == 0
                return heights[Math.min(Math.max(initializedMarkers - 1, 0), (int)(initializedMarkers * percentile))];
            }
            return heights[2];
        }

        public synchronized void decay(final double decayMultiplier) {
            if (decayMultiplier != 1 && initializedMarkers == N_MARKERS) {
                for (int i = 0; i < idealPositions.length; i++) {
                    idealPositions[i] *= decayMultiplier;
                    positions[i+1] *= decayMultiplier;
                }
                // Move max / min toward eachother according to decayMultiplier
                final double rise = heights[N_MARKERS - 1] - heights[0];
                final double distance = (1 - decayMultiplier) * rise;
                heights[0] += distance / 2;
                heights[N_MARKERS - 1] -= distance / 2;
                // shove other markers inward if needed
                for (int i = 0; i < N_MARKERS / 2; i++) {
                    if (heights[i] >= heights[i + 1]) {
                        heights[i+1] += Math.ulp(heights[i+1]);
                    }
                }
                for (int i = N_MARKERS - 1; i > N_MARKERS / 2; i--) {
                    if (heights[i] <= heights[i - 1]) {
                        heights[i - 1] -= Math.ulp(heights[i-1]);
                    }
                }
            }
        }

        /**
         * Adds another datum
         */
        public synchronized void add(final double item) {
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
            }
            if (heights[0] > item) {
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
                final double position = positions[i];
                final double positionDelta = idealPositions[i - 1] - position;

                if ((positionDelta >= 1 && positions[i + 1] > position + 1) ||
                        (positionDelta <= -1 && positions[i - 1] < position - 1)) {
                    final int direction = positionDelta > 0 ? 1 : -1;

                    final double heightBelow = heights[i - 1];
                    final double height = heights[i];
                    final double heightAbove = heights[i + 1];
                    final double positionBelow = positions[i - 1];
                    final double positionAbove = positions[i + 1];
                    final double newHeight = calcP2(direction, heightBelow, height, heightAbove,
                                                    positionBelow, position, positionAbove);

                    if (heightBelow < newHeight && newHeight < heightAbove) {
                        heights[i] = newHeight;
                    } else {
                        // use linear form
                        final double rise = heights[i + direction] - height;
                        final double run = positions[i + direction] - position;
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
                                     final /* n(i-1) */ double positionBelow,
                                     final /* n(i)   */ double position,
                                     final /* n(i+1) */ double positionAbove) {
            // q + d / (n(i+1) - n(i-1) *
            //     ((n - n(i-1) + d) * (q(i+1) - q) / (n(i+1) - n) + (n(i+1) - n - d) * (q - q(i-1)) / (n - n(i-1)))
            final double xBelow = position - positionBelow;
            final double xAbove = positionAbove - position;
            final double belowScale = (xAbove - direction) / xBelow;
            final double aboveScale = (xBelow + direction) / xAbove;
            final double lowerHalf = belowScale * (height - heightBelow);
            final double upperHalf = aboveScale * (heightAbove - height);
            return height + Math.copySign((upperHalf + lowerHalf) / (positionAbove - positionBelow), direction);
        }
    }
}
