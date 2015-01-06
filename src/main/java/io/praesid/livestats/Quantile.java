package io.praesid.livestats;

import lombok.ToString;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import java.util.Arrays;
import java.util.concurrent.locks.StampedLock;

@ThreadSafe
@ToString
final class Quantile {
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
