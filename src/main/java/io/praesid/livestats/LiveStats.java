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
import java.util.concurrent.locks.StampedLock;
import java.util.function.DoubleConsumer;

@ThreadSafe
@ToString
public final class LiveStats implements DoubleConsumer {

    private final StampedLock lock = new StampedLock();

    @GuardedBy("lock")
    private double min = Double.POSITIVE_INFINITY;
    @GuardedBy("lock")
    private double decayedMin = Double.POSITIVE_INFINITY;
    @GuardedBy("lock")
    private double max = Double.NEGATIVE_INFINITY;
    @GuardedBy("lock")
    private double decayedMax = Double.NEGATIVE_INFINITY;
    @GuardedBy("lock")
    private double sum = 0;
    @GuardedBy("lock")
    private double sumCentralMoment2 =  0;
    @GuardedBy("lock")
    private double sumCentralMoment3 = 0;
    @GuardedBy("lock")
    private double sumCentralMoment4 = 0;
    @GuardedBy("lock")
    private long count = 0;
    @GuardedBy("lock")
    private double decayedCount = 0;
    @GuardedBy("lock")
    private int decayCount = 0;

    private final ImmutableList<Quantile> quantiles;
    private final long startNanos = System.nanoTime();
    private final double decayMultiplier;
    private final long decayPeriodNanos;

    /**
     * Constructs a LiveStats object which will track stats for all time
     *
     * @param quantiles Quantiles to track (eg. .5 for the 50th percentile)
     */
    public LiveStats(final double... quantiles) {
        this(1, Duration.ofMillis(0), quantiles);
    }

    /**
     * Constructs a LiveStats object which will exponentially decay collected stats over time
     *
     * @param decayMultiplier the multiplier by which to reduce values when decaying
     * @param decayPeriod the time between decay events
     * @param quantiles Quantiles to track (eg. .5 for the 50th percentile)
     */
    public LiveStats(double decayMultiplier, Duration decayPeriod, final double... quantiles) {
        final Builder<Quantile> tilesBuilder = ImmutableList.builder();
        for (final double tile : quantiles) {
            tilesBuilder.add(new Quantile(tile));
        }
        this.quantiles = tilesBuilder.build();
        this.decayMultiplier = decayMultiplier;
        decayPeriodNanos = decayPeriod.toNanos();
    }

    public void decay() {
        if (decayMultiplier == 1) {
            return;
        }
        final int expectedDecays = (int)((System.nanoTime() - startNanos) / decayPeriodNanos);
        final long optimisticStamp = lock.tryOptimisticRead();
        int myDecayCount = decayCount;
        if (!lock.validate(optimisticStamp)) {
            final long readStamp = lock.readLock();
            myDecayCount = decayCount;
            lock.unlock(readStamp);
        }
        if (expectedDecays == myDecayCount) {
            return;
        }
        final double myDecayMultiplier;
        final long writeStamp = lock.writeLock();
        try {
            myDecayMultiplier = Math.pow(decayMultiplier, expectedDecays - decayCount);
            if (count != 0) { // These turn into Double.NaN if decay happens while they're infinite
                final double minMaxDecay = (decayedMax - decayedMin) * (myDecayMultiplier / 2);
                decayedMin += minMaxDecay;
                decayedMax -= minMaxDecay;
            }
            sum *= myDecayMultiplier;
            decayedCount *= myDecayMultiplier;
            sumCentralMoment2 *= myDecayMultiplier;
            sumCentralMoment3 *= myDecayMultiplier;
            sumCentralMoment4 *= myDecayMultiplier;
            decayCount = expectedDecays;
        } finally {
            lock.unlock(writeStamp);
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

        final double targetMin;
        final double targetMax;
        final long stamp = lock.writeLock();
        try {
            min = Math.min(min, item);
            targetMin = decayedMin = Math.min(decayedMin, item);
            max = Math.max(max, item);
            targetMax = decayedMax = Math.max(decayedMax, item);

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
        } finally {
            lock.unlock(stamp);
        }

        for (final Quantile quantile : quantiles) {
            quantile.add(item, targetMin, targetMax);
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

    public double maximum() {
        final long optimisticStamp = lock.tryOptimisticRead();
        double maximum = max;
        if (!lock.validate(optimisticStamp)) {
            final long readStamp = lock.readLock();
            maximum = max;
            lock.unlock(readStamp);
        }
        return maximum;
    }

    public double decayedMaximum() {
        final long optimisticStamp = lock.tryOptimisticRead();
        double decayedMaximum = decayedMax;
        if (!lock.validate(optimisticStamp)) {
            final long readStamp = lock.readLock();
            decayedMaximum = decayedMax;
            lock.unlock(readStamp);
        }
        return decayedMaximum;
    }

    public double mean() {
        final long optimisticStamp = lock.tryOptimisticRead();
        double mean = sum / decayedCount;
        if (!lock.validate(optimisticStamp)) {
            final long readStamp = lock.readLock();
            mean = sum / decayedCount;
            lock.unlock(readStamp);
        }
        return mean;
    }

    public double minimum() {
        final long optimisticStamp = lock.tryOptimisticRead();
        double minimum = min;
        if (!lock.validate(optimisticStamp)) {
            final long readStamp = lock.readLock();
            minimum = min;
            lock.unlock(readStamp);
        }
        return minimum;
    }

    public double decayedMinimum() {
        final long optimisticStamp = lock.tryOptimisticRead();
        double decayedMinimum = decayedMin;
        if (!lock.validate(optimisticStamp)) {
            final long readStamp = lock.readLock();
            decayedMinimum = decayedMin;
            lock.unlock(readStamp);
        }
        return decayedMinimum;
    }

    public long num() {
        final long optimisticStamp = lock.tryOptimisticRead();
        long num = count;
        if (!lock.validate(optimisticStamp)) {
            final long readStamp = lock.readLock();
            num = count;
            lock.unlock(readStamp);
        }
        return num;
    }

    public double decayedNum() {
        final long optimisticStamp = lock.tryOptimisticRead();
        double decayedNum = decayedCount;
        if (!lock.validate(optimisticStamp)) {
            final long readStamp = lock.readLock();
            decayedNum = decayedCount;
            lock.unlock(readStamp);
        }
        return decayedNum;
    }

    public int decayCount() {
        final long optimisticStamp = lock.tryOptimisticRead();
        int myDecayCount = decayCount;
        if (!lock.validate(optimisticStamp)) {
            final long readStamp = lock.readLock();
            myDecayCount = decayCount;
            lock.unlock(readStamp);
        }
        return myDecayCount;
    }

    public double variance() {
        final long optimisticStamp = lock.tryOptimisticRead();
        double variance = sumCentralMoment2 / decayedCount;
        if (!lock.validate(optimisticStamp)) {
            final long readStamp = lock.readLock();
            variance = sumCentralMoment2 / decayedCount;
            lock.unlock(readStamp);
        }
        return variance;
    }

    public double kurtosis() {
        final long optimisticStamp = lock.tryOptimisticRead();
        double mySumCentralMoment2 = sumCentralMoment2;
        double mySumCentralMoment4 = sumCentralMoment4;
        double myDecayedCount = decayedCount;
        if (!lock.validate(optimisticStamp)) {
            final long readStamp = lock.readLock();
            mySumCentralMoment2 = sumCentralMoment2;
            mySumCentralMoment4 = sumCentralMoment4;
            myDecayedCount = decayedCount;
            lock.unlock(readStamp);
        }
        // u4 / u2^2 - 3
        // (s4/c) / (s2/c)^2 - 3
        // s4 / (c * (s2/c)^2) - 3
        // s4 / (c * (s2/c) * (s2/c)) - 3
        // s4 / (s2^2 / c) - 3
        // s4 * c / s2^2 - 3
        if (mySumCentralMoment4 == 0) {
            return 0;
        }
        return mySumCentralMoment4 * myDecayedCount / Math.pow(mySumCentralMoment2, 2) - 3;
    }

    public double skewness() {
        final long optimisticStamp = lock.tryOptimisticRead();
        double mySumCentralMoment2 = sumCentralMoment2;
        double mySumCentralMoment3 = sumCentralMoment3;
        double myDecayedCount = decayedCount;
        if (!lock.validate(optimisticStamp)) {
            final long readStamp = lock.readLock();
            mySumCentralMoment2 = sumCentralMoment2;
            mySumCentralMoment3 = sumCentralMoment3;
            myDecayedCount = decayedCount;
            lock.unlock(readStamp);
        }
        // u3 / u2^(3/2)
        // (s3/c) / (s2/c)^(3/2)
        // s3 / (c * (s2/c)^(3/2))
        // s3 / (c * (s2/c) * (s2/c)^(1/2))
        // s3 / (s2 * sqrt(s2/c))
        // s3 * sqrt(c/s2) / s2
        if (mySumCentralMoment3 == 0) {
            return 0;
        }
        return mySumCentralMoment3 * Math.sqrt(myDecayedCount / mySumCentralMoment2) / mySumCentralMoment2;
    }

    @ThreadSafe
    @ToString
    private static final class Quantile {
        private static final int N_MARKERS = 5; // positionDeltas and idealPositions must be updated if this is changed

        private final StampedLock lock = new StampedLock();
        private final StampedLock initLock = new StampedLock();

        // length of positionDeltas and idealPositions is N_MARKERS-1 because the lowest idealPosition is always 1
        private final double[] positionDeltas; // Immutable, how far the ideal positions move for each item
        @GuardedBy("lock")
        private final double[] idealPositions;
        @GuardedBy("lock")
        private final double[] positions = {1, 2, 3, 4, 5};
        @GuardedBy("lock")
        private final double[] heights = new double[N_MARKERS];
        @GuardedBy("initLock,lock") // guarded by both write locks, so either read lock guarantees visibility
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

        public double quantile() {
            sortIfNeeded();
            final long optimisticStamp = lock.tryOptimisticRead();
            double quantile = heights[initializedMarkers / 2];
            if (!lock.validate(optimisticStamp)) {
                final long readStamp = lock.readLock();
                quantile = heights[initializedMarkers / 2];
                lock.unlock(readStamp);
            }
            return quantile;
        }

        public void decay(final double decayMultiplier) {
            if (decayMultiplier == 1) {
                return;
            }
            final long writeStamp = lock.writeLock();
            if (initializedMarkers == N_MARKERS) {
                for (int i = 0; i < idealPositions.length; i++) {
                    idealPositions[i] *= decayMultiplier;
                    positions[i + 1] *= decayMultiplier;
                }
            }
            lock.unlock(writeStamp);
        }

        /**
         * Adds another datum
         */
        public void add(final double item, final double targetMin, final double targetMax) {
            final long writeStamp = lock.writeLock();
            try {
                if (initializedMarkers < N_MARKERS) {
                    heights[initializedMarkers] = item;
                    final long initWriteStamp = initLock.writeLock();
                    initializedMarkers++;
                    initLock.unlock(initWriteStamp);
                    if (initializedMarkers == N_MARKERS) {
                        Arrays.sort(heights);
                    }
                    return;
                }

                if (targetMax > heights[N_MARKERS - 2]) {
                    heights[N_MARKERS - 1] = targetMax;
                } else {
                    heights[N_MARKERS - 1] = heights[N_MARKERS - 2] + Math.ulp(heights[N_MARKERS - 2]);
                }
                if (targetMin < heights[1]) {
                    heights[0] = targetMin;
                } else {
                    heights[0] = heights[1] - Math.ulp(heights[1]);
                }
                positions[N_MARKERS - 1]++; // Because marker N_MARKERS-1 is max, it always gets incremented
                for (int i = N_MARKERS - 2; heights[i] > item; i--) { // Increment all other markers > item
                    positions[i]++;
                }

                for (int i = 0; i < idealPositions.length; i++) {
                    idealPositions[i] += positionDeltas[i]; // updated desired positions
                }

                adjust();
            } finally {
                lock.unlock(writeStamp);
            }
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

        private void sortIfNeeded() {
            final long optimisticStamp = initLock.tryOptimisticRead();
            int myInitializedMarkers = initializedMarkers;
            if (!initLock.validate(optimisticStamp)) {
                final long readStamp = initLock.readLock();
                myInitializedMarkers = initializedMarkers;
                initLock.unlock(readStamp);
            }
            if (myInitializedMarkers < N_MARKERS) {
                final long writeStamp = lock.writeLock();
                Arrays.sort(heights, 0, initializedMarkers);
                lock.unlock(writeStamp);
            }
        }
    }
}
