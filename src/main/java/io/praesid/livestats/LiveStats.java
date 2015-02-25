package io.praesid.livestats;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableMap;
import lombok.ToString;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
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
    private final DecayConfig decayConfig;

    /**
     * Constructs a LiveStats object which will track stats for all time
     *
     * @param quantiles Quantiles to track (eg. .5 for the 50th percentile)
     */
    public LiveStats(final double... quantiles) {
        this(DecayConfig.NEVER, quantiles);
    }

    /**
     * Constructs a LiveStats object which will exponentially decay collected stats over time
     *
     * @param decayConfig the configuration for time-based decay of statistics
     * @param quantiles Quantiles to track (eg. .5 for the 50th percentile)
     */
    public LiveStats(final DecayConfig decayConfig, final double... quantiles) {
        final Builder<Quantile> tilesBuilder = ImmutableList.builder();
        for (final double tile : quantiles) {
            tilesBuilder.add(new Quantile(tile));
        }
        this.quantiles = tilesBuilder.build();
        this.decayConfig = decayConfig;
    }

    /**
     * Decays the currently recorded stats as much as they currently should be.
     *
     * The only time you need to call this directly is before reading stats.
     * In general, it's called automatically as you add items.
     */
    public void decayByTime() {
        if (decayConfig.multiplier == 1 || decayConfig.period == 0) {
            return;
        }
        final int expectedDecays = (int)((System.nanoTime() - startNanos) / decayConfig.period);
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
        decayTo(lock.writeLock(), expectedDecays);
    }

    public void decay() {
        if (decayConfig.multiplier == 1 || decayConfig.period != 0) {
            return;
        }
        final long writeStamp = lock.writeLock();
        decayTo(writeStamp, decayCount+1);
    }

    private void decayTo(final long writeStamp, final int expectedDecays) {
        final double myDecayMultiplier;
        try {
            myDecayMultiplier = Math.pow(decayConfig.multiplier, expectedDecays - decayCount);
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
        decayByTime();

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

    /**
     * @return The maximum value.
     */
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

    /**
     * Gets the decayed maximum value of items so far added.  This is not generally very useful, but could
     * be used to drive some kind of peak meter or something.  This value tends to jump around a lot as new
     * maximums come in and decay periods hit.
     *
     * @return The decayed maximum.
     */
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

    /**
     * @return The current decayed mean.
     */
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

    /**
     * @return The maximum value.
     */
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

    /**
     * Gets the decayed minimum value of items so far added.  This is not generally very useful, but could
     * be used to drive some kind of peak meter or something.  This value tends to jump around a lot as new
     * minimums come in and decay periods hit.
     *
     * @return The decayed minimum.
     */
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

    /**
     * @return The total number of items.
     */
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

    /**
     * @return The approximate number of items counted in decayed stats.
     */
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

    /**
     * @return The number of times that stats have been decayed.
     */
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

    /**
     * @return The current decayed variance.
     */
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

    /**
     * @return The current decayed kurtosis.
     */
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

    /**
     * @return The current decayed skewness.
     */
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

}
